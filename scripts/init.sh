#!/usr/bin/env bash

# Обработка ошибок
set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RESET='\e[0m'

# ==========================================
# Переменные проекта
# ==========================================
ROOT_DIR="/root/web"
DB_DIR="$ROOT_DIR/src/data/databases"
LOGS_DIR="$ROOT_DIR/src/data/logs"
HTTPS_DIR="$ROOT_DIR/src/data/https"
HTTPS_SELF_DIR="$HTTPS_DIR/self-cert"
HTTPS_LE_DIR="$HTTPS_DIR/letsencrypt"
DEFAULT_PORT=1234
ENV_FILE="$ROOT_DIR/src/data/.env"
SERVICE_FILE="/etc/supervisord.conf"
VNSTAT_CONF_FILE="/etc/vnstat.conf"
NEW_DATABASE_DIR="$DB_DIR/vnstat"

# ==========================================
# Автоматические параметры (ENV)
# ==========================================
PORT=${PORT:-$DEFAULT_PORT}
HTTPS_ON=${HTTPS_ON:-"N"}
DOMAIN_NAME=${DOMAIN_NAME:-""}
BOT_ON=${BOT_ON:-"N"}
BOT_TOKEN=${BOT_TOKEN:-""}
ADMIN_ID=${ADMIN_ID:-""}

# ==========================================
# Определение IP-адреса сервера
# ==========================================
get_server_ip() {
    local ip=$(curl -s http://checkip.amazonaws.com 2>/dev/null || echo "")
    if [[ -z "$ip" ]]; then
        ip=$(hostname -I | awk '{print $1}' 2>/dev/null || echo "127.0.0.1")
    fi
    echo "$ip"
}

SERVER_IP=$(get_server_ip)
echo -e "${GREEN}Server IP detected: $SERVER_IP${RESET}"

# ==========================================
# Функции HTTPS
# ==========================================
save_setup_var() {
    local key=$1
    local value=$2
    if [[ -f "$ENV_FILE" ]]; then
        if grep -q "^${key}=" "$ENV_FILE"; then
            sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
        else
            echo "${key}=${value}" >> "$ENV_FILE"
        fi
    fi
    export "$key"="$value"
}

# Функция обновления привязки Gunicorn
update_service_ip() {
    local new_ip=$1
    local flask_port=$2
    # 🔥 При HTTPS используем DEFAULT_PORT для Gunicorn
    if [[ "$HTTPS_ON" =~ ^[Yy]$ ]]; then
        flask_port=$DEFAULT_PORT
    fi
    if [[ -f "$SERVICE_FILE" ]]; then
        sed -i "s|command=gunicorn -w 4 main:app -b .*:[0-9]*|command=gunicorn -w 4 main:app -b $new_ip:$flask_port|" "$SERVICE_FILE"
        echo -e "${GREEN}Привязка сервиса обновлена $new_ip:$flask_port${RESET}"
    fi
}

generate_self_signed_cert() {
    local cert_domain=$1
    local cert_ip=$2
    local cert_path="$HTTPS_SELF_DIR/selfsigned.crt"
    local key_path="$HTTPS_SELF_DIR/selfsigned.key"
    
    echo -e "${YELLOW}🔒 Генерируем самоподписанный сертификат...${RESET}"
    mkdir -p "$HTTPS_SELF_DIR"
    
    openssl req -x509 -nodes -days 3650 -newkey rsa:2048 \
        -keyout "$key_path" \
        -out "$cert_path" \
        -subj "/C=US/ST=State/L=City/O=Organization/CN=$cert_domain" \
        -addext "subjectAltName=DNS:$cert_domain,DNS:localhost,IP:$cert_ip"
    
    chmod 600 "$key_path"
    chmod 644 "$cert_path"
    
    echo -e "${GREEN}Самоподписанный сертификат создан в $cert_path${RESET}"
    echo -e "${YELLOW}Сертификат включает IP-адрес: $cert_ip${RESET}"
    
    CERT_PATH="$cert_path"
    KEY_PATH="$key_path"
    CERT_TYPE="self-signed"
}

copy_letsencrypt_cert() {
    local domain=$1
    local cert_path="$HTTPS_LE_DIR/${domain}_fullchain.pem"
    local key_path="$HTTPS_LE_DIR/${domain}_privkey.pem"
    
    echo -e "${YELLOW}📋 Копируем сертификаты Let's Encrypt...${RESET}"
    mkdir -p "$HTTPS_LE_DIR"
    
    cp "/etc/letsencrypt/live/$domain/fullchain.pem" "$cert_path"
    cp "/etc/letsencrypt/live/$domain/privkey.pem" "$key_path"
    
    chmod 644 "$cert_path"
    chmod 600 "$key_path"
    
    echo -e "${GREEN}Сертификат скопирован в $HTTPS_LE_DIR${RESET}"
    
    CERT_PATH="$cert_path"
    KEY_PATH="$key_path"
    CERT_TYPE="letsencrypt"
}

check_nginx_configs() {
    local sites_available="/etc/nginx/sites-available"
    local target_domain=$1
    STATUSOPENVPN_CONFIGS=()
    OTHER_CONFIGS=()
    DOMAIN_CONFIG=""
    
    for config_file in "$sites_available"/*; do
        [[ ! -f "$config_file" ]] && continue
        local basename_config=$(basename "$config_file")
        [[ "$basename_config" == "default" ]] && continue
        local first_line=$(head -n 1 "$config_file" 2>/dev/null)
        
        if [[ "$first_line" == "# Created by StatusOpenVPN" ]]; then
            STATUSOPENVPN_CONFIGS+=("$config_file")
            if [[ "$basename_config" == "$target_domain" ]]; then
                DOMAIN_CONFIG="$config_file"
            fi
        else
            OTHER_CONFIGS+=("$config_file")
        fi
    done
}

check_dependencies() {
    local missing=()
    
    if ! command -v nginx &> /dev/null; then
        missing+=("nginx")
    fi
    if ! command -v openssl &> /dev/null; then
        missing+=("openssl")
    fi
    if ! command -v certbot &> /dev/null; then
        echo -e "${YELLOW}⚠️  certbot не найден. Сертификаты Let's Encrypt будут недоступны.${RESET}"
    fi
    
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo -e "${RED}❌ Отсутствуют необходимые приложения: ${missing[*]}${RESET}"
        echo -e "${YELLOW}Переключаемся на самоподписанный сертификат...${RESET}"
        return 1
    fi
    return 0
}

setup_https() {
    local domain=$1
    local use_self_signed=$2
    local flask_port=$3
    local https_port=$4
    local server_ip=$5
    
    echo -e "${YELLOW}🔧 Настраиваем HTTPS для $domain (Port: $https_port)...${RESET}"
    mkdir -p "$HTTPS_DIR"
    
    if ! check_dependencies; then
        use_self_signed="true"
        domain="$server_ip"
    fi
    
    if [[ "$use_self_signed" != "true" && ! "$domain" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        if command -v curl &> /dev/null; then
            DOMAIN_IP=$(getent ahostsv4 "$domain" 2>/dev/null | awk '{print $1}' | head -n1 || echo "")
            if [[ -n "$DOMAIN_IP" && "$server_ip" != "$DOMAIN_IP" ]]; then
                echo -e "${RED}⚠️  Предупреждение: IP-адрес домена ($DOMAIN_IP) не совпадает с IP-адресом сервера ($server_ip).${RESET}"
                echo -e "${YELLOW}Переключаемся на самоподписанный сертификат...${RESET}"
                use_self_signed="true"
                domain="$server_ip"
            fi
        fi
    fi
    
    mkdir -p /etc/nginx/sites-available /etc/nginx/sites-enabled
    
    if [[ "$use_self_signed" == "true" ]]; then
        generate_self_signed_cert "$domain" "$server_ip"
    else
        if certbot certificates 2>/dev/null | grep -q "Domains: $domain"; then
            echo -e "${GREEN}Сертификат уже существует для $domain${RESET}"
            copy_letsencrypt_cert "$domain"
        else
            echo -e "${YELLOW}Получение нового сертификата от Let's Encrypt...${RESET}"
            EMAIL="admin@$domain"
            certbot --nginx -d "$domain" --email "$EMAIL" --agree-tos --non-interactive || {
                echo -e "${RED}Certbot завершился ошибкой. Переключаемся на самоподписанный сертификат...${RESET}"
                use_self_signed="true"
                domain="$server_ip"
                generate_self_signed_cert "$domain" "$server_ip"
            }
            if [[ "$use_self_signed" != "true" ]]; then
                copy_letsencrypt_cert "$domain"
            fi
        fi
    fi
    
    check_nginx_configs "$domain"
    
    local update_existing=false
    local disable_default=false
    
    if [[ -n "$DOMAIN_CONFIG" && ${#STATUSOPENVPN_CONFIGS[@]} -eq 1 && ${#OTHER_CONFIGS[@]} -eq 0 ]]; then
        echo -e "${YELLOW}Обнаружен конфигурационный файл StatusOpenVPN. Обновляем...${RESET}"
        update_existing=true
        disable_default=true
    elif [[ ${#STATUSOPENVPN_CONFIGS[@]} -eq 0 && ${#OTHER_CONFIGS[@]} -eq 0 ]]; then
        echo -e "${YELLOW}Отсутствует конфигурация. Создаем новую...${RESET}"
        disable_default=true
    fi
    
    local NGINX_CONF="/etc/nginx/sites-available/$domain"
    local NGINX_LINK="/etc/nginx/sites-enabled/$domain"
    local config_content
    
    config_content=$(cat <<EOF
# Created by StatusOpenVPN
server {
    listen 80;
    server_name $domain;
    return 301 https://\$host:$https_port\$request_uri;
}
server {
    listen $https_port ssl;
    server_name $domain;
    ssl_certificate     $CERT_PATH;
    ssl_certificate_key $KEY_PATH;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    location / {
        proxy_pass http://127.0.0.1:$DEFAULT_PORT;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Script-Name /;
        proxy_redirect off;
    }
}
EOF
)
    
    if [[ "$update_existing" == true ]]; then
        echo "$config_content" > "$DOMAIN_CONFIG"
        NGINX_CONF="$DOMAIN_CONFIG"
    else
        echo "$config_content" > "$NGINX_CONF"
    fi
    
    ln -sf "$NGINX_CONF" "$NGINX_LINK"
    
    if [[ "$disable_default" == true ]]; then
        local default_link="/etc/nginx/sites-enabled/default"
        if [[ -L "$default_link" ]]; then
            rm -f "$default_link"
            echo -e "${GREEN}Default сайт отключен.${RESET}"
        fi
    fi
    
    if nginx -t; then
        echo -e "${GREEN}Тестирование конфигурации Nginx завершено успешно.${RESET}"
    else
        echo -e "${RED}Тестирование конфигурации Nginx завершилось ошибкой!${RESET}"
        exit 1
    fi
    
    save_setup_var "HTTPS_ENABLED" "1"
    save_setup_var "DOMAIN" "$domain"
    save_setup_var "HTTPS_DIR" "$HTTPS_DIR"
    save_setup_var "HTTPS_PORT" "$https_port"
    save_setup_var "SERVER_IP" "$server_ip"
    save_setup_var "CERT_TYPE" "$CERT_TYPE"
    
    if [[ "$use_self_signed" == "true" ]]; then
        save_setup_var "SELF_SIGNED" "1"
    fi
    
    echo -e "${GREEN}✅ Настройка HTTPS завершена. Приложение доступно по адресу: https://$domain:$https_port/${RESET}"
    echo -e "${GREEN}Тип сертификата: $CERT_TYPE${RESET}"
    echo -e "${GREEN}Путь к сертификату: $CERT_PATH${RESET}"
    
    if [[ "$use_self_signed" == "true" ]]; then
        echo -e "${YELLOW}⚠️  Заметка: Самоподписанный сертификат — примите предупреждение браузера.${RESET}"
    fi
}

# ==========================================
# Основная логика инициализации
# ==========================================
echo -e "${GREEN}🚀 Запускаем инициализацию StatusOpenVPN...${RESET}"

# Создаем папки
mkdir -p "$DB_DIR"
mkdir -p "$LOGS_DIR"
mkdir -p "$NEW_DATABASE_DIR"

# Создаем HTTPS директорию если нужно
if [[ "$HTTPS_ON" =~ ^[Yy]$ ]]; then
    mkdir -p "$HTTPS_DIR"
    echo -e "${GREEN}HTTPS папка создана: $HTTPS_DIR${RESET}"
fi

# ==========================================
# Создание supervisord.conf
# ==========================================
if [ ! -f "$SERVICE_FILE" ]; then
    # 🔥 Определяем порт для Gunicorn
    if [[ "$HTTPS_ON" =~ ^[Yy]$ ]]; then
        GUNICORN_PORT=$DEFAULT_PORT  # Внутренний порт для Gunicorn
        GUNICORN_BIND="127.0.0.1:$GUNICORN_PORT"
    else
        GUNICORN_PORT=$PORT  # Внешний порт (без HTTPS)
        GUNICORN_BIND="0.0.0.0:$GUNICORN_PORT"
    fi
    
    cat <<EOF | tee $SERVICE_FILE
[supervisord]
user=root
nodaemon=true

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[unix_http_server]
file=/var/run/supervisor.sock
chmod=0700

[supervisorctl]
serverurl=unix:///var/run/supervisor.sock


# ==========================================
# Добавление main.py в Supervisor
# ==========================================
[program:gunicorn]
command=gunicorn -w 4 main:app -b $GUNICORN_BIND
directory=$ROOT_DIR
autostart=true
autorestart=true
stderr_logfile=$LOGS_DIR/gunicorn.stderr.log
stderr_logfile_maxbytes=10MB
stderr_logfile_backups=5


# ==========================================
# Добавление Logs.py в Supervisor
# ==========================================
[program:logs]
command=/bin/sh -c "sleep 30 && while true; do /usr/local/bin/python $ROOT_DIR/src/logs.py; sleep 30; done"
directory=$ROOT_DIR/src
autostart=true
autorestart=true
EOF
    
    echo -e "${GREEN}Базовая настройка Supervisord завершена.${RESET}"
else
    echo "SERVICE_FILE существует, пропускаем создание файла."
fi

# ==========================================
# Добавление Nginx в Supervisor (если HTTPS)
# ==========================================
if [[ "$HTTPS_ON" =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}🔐 HTTPS включен. Добавляем Nginx в Supervisor...${RESET}"
    
    # Добавляем конфигурацию Nginx в supervisord.conf
    cat <<EOF >> $SERVICE_FILE
[program:nginx]
user=root
command=nginx -g 'daemon off;'
autostart=true
autorestart=true
stdout_logfile=$LOGS_DIR/nginx.stdout.log
stderr_logfile=$LOGS_DIR/nginx.stderr.log
stdout_logfile_maxbytes=10MB
stderr_logfile_maxbytes=10MB
stdout_logfile_backups=5
stderr_logfile_backups=5
EOF
    
    echo -e "${GREEN}Служба Nginx добавлена в Supervisor.${RESET}"
fi

# ==========================================
# Telegram Bot
# ==========================================
if [[ "$BOT_ON" =~ ^[Yy]$ ]]; then
    cat <<EOF | tee -a $SERVICE_FILE >/dev/null
[program:telegram-bot]
command=/usr/local/bin/python $ROOT_DIR/src/vpn_bot.py
directory=$ROOT_DIR/src
autostart=true
autorestart=true
startretries=3
startsecs=300
restartpause=10
EOF
    
    if [ ! -f "$ENV_FILE" ]; then
        echo "Creating .env file at $ENV_FILE..."
        cat <<EOF > $ENV_FILE
BOT_TOKEN=$BOT_TOKEN
ADMIN_ID=$ADMIN_ID
EOF
    else
        echo ".env файл существует. Пропускаем создание."
    fi
    
    echo -e "${GREEN}Telegram bot configured.${RESET}"
fi

# ==========================================
# Настройка HTTPS
# ==========================================
if [[ "$HTTPS_ON" =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}🔐 Настраиваем HTTPS...${RESET}"
    USE_SELF_SIGNED="false"
    
    # Определяем тип сертификата
    if [[ -z "$DOMAIN_NAME" ]]; then
        USE_SELF_SIGNED="true"
        DOMAIN_NAME="$SERVER_IP"
        echo -e "${YELLOW}Домен отсутствует. Используем самоподписанный сертификат для IP: $SERVER_IP${RESET}"
    elif [[ "$DOMAIN_NAME" == "localhost" ]] || [[ "$DOMAIN_NAME" == "127.0.0.1" ]]; then
        USE_SELF_SIGNED="true"
        DOMAIN_NAME="$SERVER_IP"
        echo -e "${YELLOW}Обнаружен Localhost. Используем самоподписанный сертификат для IP: $SERVER_IP${RESET}"
    else
        echo -e "${GREEN}Домен указан: $DOMAIN_NAME. Пробуем получить сертификат Let's Encrypt...${RESET}"
    fi
    
    setup_https "$DOMAIN_NAME" "$USE_SELF_SIGNED" "$PORT" "$PORT" "$SERVER_IP"
    
    # Обновляем привязку Gunicorn на localhost (только Nginx может обращаться)
    update_service_ip "127.0.0.1" "$DEFAULT_PORT"
    
    # ==========================================
    # Добавление мониторинга сертификатов (ТОЛЬКО если есть домен)
    # ==========================================
    # Проверяем, что это НЕ самоподписанный сертификат и домен не является IP
    if [[ "$USE_SELF_SIGNED" != "true" ]] && [[ ! "$DOMAIN_NAME" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo -e "${YELLOW}🔒 Настраиваем службу автообновления сертификата...${RESET}"
        
        # Создаем скрипт мониторинга
        cat <<'MONITOR_SCRIPT' > $ROOT_DIR/scripts/cert_monitor.sh
#!/usr/bin/env bash
set -e
ROOT_DIR="/root/web"
ENV_FILE="$ROOT_DIR/src/data/.env"
HTTPS_DIR="$ROOT_DIR/src/data/https"
HTTPS_LE_DIR="$HTTPS_DIR/letsencrypt"
LOG_FILE="$LOGS_DIR/cert_renew.log"
RENEW_THRESHOLD=30
CHECK_INTERVAL=86400

log() { 
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"
}

check_cert() {
    local cert_path=$1
    [[ ! -f "$cert_path" ]] && return 1
    local expiry=$(openssl x509 -enddate -noout -in "$cert_path" 2>/dev/null | cut -d= -f2)
    [[ -z "$expiry" ]] && return 1
    local expiry_epoch=$(date -d "$expiry" +%s)
    local now=$(date +%s)
    local days=$(( (expiry_epoch - now) / 86400 ))
    log "ℹ️ Сертификат истекает через $days дней"
    [[ $days -lt 0 ]] && return 1
    [[ $days -lt $RENEW_THRESHOLD ]] && return 2
    return 0
}

renew_cert() {
    local domain=$1
    log "🔄 Обновляем сертифкат для домена $domain..."
    certbot renew --non-interactive --quiet 2>/dev/null || return 1
    log "✅ Сертификат обновлен"
}

copy_certs() {
    local domain=$1
    local src_cert="/etc/letsencrypt/live/$domain/fullchain.pem"
    local src_key="/etc/letsencrypt/live/$domain/privkey.pem"
    local dst_dir="$HTTPS_LE_DIR"
    [[ ! -f "$src_cert" ]] || [[ ! -f "$src_key" ]] && return 1
    mkdir -p "$dst_dir"
    cp "$src_cert" "$dst_dir/${domain}_fullchain.pem"
    cp "$src_key" "$dst_dir/${domain}_privkey.pem"
    chmod 644 "$dst_dir/${domain}_fullchain.pem"
    chmod 600 "$dst_dir/${domain}_privkey.pem"
    log "✅ Сертификат скопирован"
}

reload_nginx() {
    nginx -t 2>/dev/null || return 1
    supervisorctl restart nginx 2>/dev/null || systemctl reload nginx 2>/dev/null || service nginx reload 2>/dev/null || return 1
    log "✅ Nginx перезапущен"
}

# Main Loop
[[ ! -f "$ENV_FILE" ]] && { log "❌ .env не найден"; exit 1; }
DOMAIN=$(grep "^DOMAIN=" "$ENV_FILE" 2>/dev/null | cut -d= -f2)
[[ -z "$DOMAIN" ]] && { log "❌ Домен не настроен"; exit 1; }
log "🔒 Монитор сертификата для $DOMAIN запущен"

while true; do
    cert_file="$HTTPS_LE_DIR/${DOMAIN}_fullchain.pem"
    if ! check_cert "$cert_file"; then
        status=$?
        if [[ $status -eq 1 ]] || [[ $status -eq 2 ]]; then
            log "⚠️ Требуется обновление сертификата"
            renew_cert "$DOMAIN" && copy_certs "$DOMAIN" && reload_nginx || log "❌ Обновление завершено с ошибкой"
        fi
    else
        log "✅ Сертификат действующий"
    fi
    sleep $CHECK_INTERVAL
done
MONITOR_SCRIPT
        
        chmod +x $ROOT_DIR/scripts/cert_monitor.sh
        
        # Добавляем программу в Supervisor
        cat <<EOF >> $SERVICE_FILE
[program:cert-monitor]
command=$ROOT_DIR/scripts/cert_monitor.sh
directory=$ROOT_DIR/scripts
autostart=true
autorestart=true
startretries=3
startsecs=10
stdout_logfile=$LOGS_DIR/cert_monitor.stdout.log
stderr_logfile=$LOGS_DIR/cert_monitor.stderr.log
stdout_logfile_maxbytes=10MB
stderr_logfile_maxbytes=10MB
stdout_logfile_backups=5
stderr_logfile_backups=5
EOF
        
        echo -e "${GREEN}Мониторинг сертификата добавлен в Supervisor${RESET}"
    else
        echo -e "${YELLOW}ℹ️  Самоподписанный сертификат не нуждается в обновлении${RESET}"
    fi
else
    # Обновляем привязку Gunicorn на все интерфейсы (прямой доступ)
    update_service_ip "0.0.0.0" "$PORT"
    save_setup_var "HTTPS_ENABLED" "0"
fi

# ==========================================
# Настройка vnStat
# ==========================================
echo -e "${YELLOW}📊 Настройка vnStat...${RESET}"
sed -i 's/^USER=vnstat/USER=root/' /etc/init.d/vnstat

if [ -f "$VNSTAT_CONF_FILE" ]; then
    echo "Корректировка файла $VNSTAT_CONF_FILE..."
    sed -i 's|^;\?UseLogging.*|UseLogging 1|' "$VNSTAT_CONF_FILE"
    sed -i "s|^;\?LogFile.*|LogFile \"$ROOT_DIR/src/data/logs/vnstat.log\"|" "$VNSTAT_CONF_FILE"
    sed -i 's|^;\?DatabaseDir.*|DatabaseDir "'"$NEW_DATABASE_DIR"'"|' "$VNSTAT_CONF_FILE"
    sed -i 's|^;\?AlwaysAddNewInterfaces.*|AlwaysAddNewInterfaces 0|' "$VNSTAT_CONF_FILE"
    echo "Файл $VNSTAT_CONF_FILE успешно скорректирован."
else
    echo "Файл $VNSTAT_CONF_FILE не найден."
fi

vnstatd --initdb 2>/dev/null || echo "vnstatd уже инициализирован"

for iface in $(ip -o link show | awk -F': ' '{print $2}' | cut -d'@' -f1); do
    if [[ "$iface" =~ ^(eth|ens) ]]; then
        if ! vnstat --dbiflist 2>/dev/null | grep -qw "$iface"; then
            echo "Интерфейс $iface не найден в vnStat. Добавляем..."
            vnstat --add -i "$iface" && echo "Интерфейс $iface добавлен." || echo "Ошибка добавления $iface."
        else
            echo "Интерфейс $iface уже существует в vnStat."
        fi
    fi
done

service vnstat start 2>/dev/null || echo "Служба vnstat уже запущена"

# ==========================================
# Завершение
# ==========================================
echo -e "${GREEN}========================================${RESET}"
echo -e "${GREEN}✅ Инициализация завершена!${RESET}"
echo -e "${GREEN}========================================${RESET}"

if [[ "$HTTPS_ON" =~ ^[Yy]$ ]]; then
    echo -e "${GREEN}Веб-интерфейс: https://$DOMAIN_NAME:$PORT/${RESET}"
    echo -e "${GREEN}Папка сертификата: $HTTPS_DIR${RESET}"
    echo -e "${GREEN}Тип сертификата: $CERT_TYPE${RESET}"
    echo -e "${GREEN}IP сервера: $SERVER_IP${RESET}"
    echo -e "${GREEN}Gunicorn Binding: 127.0.0.1${RESET}"
    if [[ -z "${DOMAIN_NAME}" ]] || [[ "$DOMAIN_NAME" == "$SERVER_IP" ]] || [[ "$CERT_TYPE" == "self-signed" ]]; then
        echo -e "${YELLOW}⚠️  Самоподписанный сертификат — примите предупреждение браузера${RESET}"
    fi
else
    echo -e "${GREEN}Веб-интерфейс: http://$SERVER_IP:$PORT/${RESET}"
    echo -e "${GREEN}Gunicorn Binding: 0.0.0.0${RESET}"
fi

echo -e "${GREEN}========================================${RESET}"