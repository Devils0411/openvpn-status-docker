#!/bin/bash
#
# Добавление/удаление клиента
#
# chmod +x client.sh && ./client.sh [1-8] [имя_клиента] [срок_действия]
#
# Срок действия в днях - только для OpenVPN
#
set -e

handle_error() {
	echo "$(lsb_release -ds) $(uname -r) $(date --iso-8601=seconds)"
	echo -e "\e[1;31mError at line $1: $2\e[0m"
	exit 1
}
trap 'handle_error $LINENO "$BASH_COMMAND"' ERR

# Переменные
DIR_OPENVPN=/root/web/openvpn
DIR_PKI=$DIR_OPENVPN/pki
OVPN_FILE_PATH="$DIR_OPENVPN/clients/${CLIENT_NAME}.ovpn"
export LC_ALL=C
export EASYRSA_PKI=$DIR_PKI
EASY_RSA=/usr/share/easy-rsa
INDEX="$DIR_PKI/index.txt"
umask 022

OPTION="$1"
CLIENT_NAME="$2"
CLIENT_CERT_EXPIRE="$3"
export EASYRSA_CERT_EXPIRE=1825
CERT_IP=dynamic.pool

askClientName(){
	if ! [[ "$CLIENT_NAME" =~ ^[a-zA-Z0-9_.-]{1,32}$ ]]; then
		echo
		echo 'Enter client name: 1–32 alphanumeric characters (a-z, A-Z, 0-9) with underscore (_) or dash (-) or dot (.)'
		until [[ "$CLIENT_NAME" =~ ^[a-zA-Z0-9_.-]{1,32}$ ]]; do
			read -rp 'Client name: ' -e CLIENT_NAME
		done
	fi
}

render() {
	local IFS=
	while read -r line; do
		while [[ "$line" =~ (\$\{[a-zA-Z_][a-zA-Z_0-9]*\}) ]]; do
			local LHS="${BASH_REMATCH[1]}"
			local RHS="$(eval echo "\"$LHS\"")"
			line="${line//$LHS/$RHS}"
		done
		echo "$line"
	done < "$1"
}

addOpenVPN() {
    # Check if 2FA was specified. If not - set to none.
    if [ -z "$TFA_NAME" ]; then
        TFA_NAME="none"
    fi

    # Проверяем, передан ли срок действия в третьем аргументе
    if [ -n "$CLIENT_CERT_EXPIRE" ] && [[ "$CLIENT_CERT_EXPIRE" =~ ^[0-9]+$ ]]; then
        export EASYRSA_CERT_EXPIRE="$CLIENT_CERT_EXPIRE"
        echo "Установлен срок действия сертификата: $EASYRSA_CERT_EXPIRE дней"
    else
        export EASYRSA_CERT_EXPIRE=1825
        echo "Используется срок действия по умолчанию: $EASYRSA_CERT_EXPIRE дней"
    fi

# Проверяем существование клиента (по файлу или по записи в index.txt)
    CLIENT_EXISTS=false
    if [[ -f "$OVPN_FILE_PATH" ]] || grep -q "/CN=${CLIENT_NAME}/" "$INDEX" 2>/dev/null; then
        CLIENT_EXISTS=true
    fi

    if [[ "$CLIENT_EXISTS" == true ]]; then
        echo "Клиент '$CLIENT_NAME' уже существует. Выполняется перевыпуск сертификата..."
        
        # 1. Отзываем старый сертификат
        echo "Отзываем старый сертификат..."
        $EASY_RSA/easyrsa --batch revoke "$CLIENT_NAME" 2>/dev/null || true
        
        # 2. Генерируем CRL, чтобы отзыв вступил в силу
        $EASY_RSA/easyrsa gen-crl 2>/dev/null
        
        # 3. Удаляем осиротевшие файлы старого сертификата
        cleanupOrphanedCerts "$CLIENT_NAME"
        
        # 4. Удаляем старую запись из index.txt (чтобы избежать дублей при новой генерации)
        sed -i'.bak' "/\/CN=${CLIENT_NAME}\//d" "$INDEX"
        
        echo "Старый сертификат удален. Генерируем новый..."
    else
        echo "Генерируем новый сертификат для клиента..."
    fi

    # Патч easy-rsa (если требуется)
    sed -i '/serialNumber_default/d' "$EASY_RSA/openssl-easyrsa.cnf" 2>/dev/null || true

    export EASYRSA_BATCH=1
    $EASY_RSA/easyrsa --batch --req-cn="$CLIENT_NAME" --days="$EASYRSA_CERT_EXPIRE" --req-email="$EASYRSA_REQ_EMAIL" gen-req "$CLIENT_NAME" nopass
    $EASY_RSA/easyrsa sign-req client "$CLIENT_NAME"

    # Fix for /name in index.txt
    echo "Правим БД..."
    sed -i'.bak' "$ s/$/\/name=${CLIENT_NAME}\/LocalIP=${CERT_IP}\/2FAName=${TFA_NAME}/" "$INDEX"
    echo "БД скорректирована:"
    tail -1 $INDEX

    # Certificate properties
    CA="$(cat $DIR_PKI/ca.crt)"
    CERT="$(cat $DIR_PKI/issued/${CLIENT_NAME}.crt)"
    KEY="$(cat $DIR_PKI/private/${CLIENT_NAME}.key)"
    TLS_CRYPT="$(cat $DIR_PKI/ta.key)"

    echo 'Корректируем права доступа к pki/issued...'
    chmod +r $DIR_PKI/issued

    echo 'Генерация .ovpn файла...'
    echo "$(cat $DIR_OPENVPN/config/client.conf)
<ca>
$CA
</ca>
<cert>
$CERT
</cert>
<key>
$KEY
</key>
<tls-crypt>
$TLS_CRYPT
</tls-crypt>
" > "$DIR_OPENVPN/clients/${CLIENT_NAME}.ovpn"

    echo -e "Клиентский сертификат успешно сгенерирован/обновлен!"
}

# Функция очистки осиротевших файлов сертификата
cleanupOrphanedCerts() {
    local name="$1"
    echo "Очистка старых файлов для: $name"
    
    # Удаляем файлы по имени в основных директориях
    find "$DIR_PKI" -type f \( -name "${name}.crt" -o -name "${name}.key" -o -name "${name}.req" \) -delete 2>/dev/null
    
    # Директория renewed
    rm -f "$DIR_PKI/renewed/issued/${name}.crt" 2>/dev/null
    rm -f "$DIR_PKI/renewed/private/${name}.key" 2>/dev/null
    rm -f "$DIR_PKI/renewed/reqs/${name}.req" 2>/dev/null
    
    # Директория inline
    rm -f "$DIR_PKI/inline/${name}.inline" 2>/dev/null
    
    # Директория revoked (физические файлы)
    rm -f "$DIR_PKI/revoked/issued/${name}.crt" 2>/dev/null
    rm -f "$DIR_PKI/revoked/private/${name}.key" 2>/dev/null
}

deleteOpenVPN(){
    # Получаем серийный номер из индекса
    CERT_SERIAL=$(grep -E "/name=$CLIENT_NAME/" "$INDEX" | awk '{print $3}')
    
    echo "Удаляем пользователя: $CLIENT_NAME"
    if [ -n "$CERT_SERIAL" ]; then
        echo "Серийный номер сертификата: $CERT_SERIAL"
    fi

    # ПРОВЕРКА БЕЗОПАСНОСТИ: Не выполняем, если имя пустое или равно 'ca'
    if [[ -z "$CLIENT_NAME" || "$CLIENT_NAME" == "ca" ]]; then
        echo "ОШИБКА: Неверное имя клиента, пропуск удаления файлов PKI."
        exit 1
    fi

    # 1. Удаляем *.ovpn файл конфигурации клиента
    echo "Удаляем *.ovpn файл..."
    rm -f "$OVPN_FILE_PATH"

    # 2. Удаляем файлы PKI по имени клиента во всех возможных директориях
    echo "Удаляем файлы PKI для $CLIENT_NAME..."
    
    # Основные директории
    find "$DIR_PKI" -type f \( -name "${CLIENT_NAME}.crt" -o -name "${CLIENT_NAME}.key" -o -name "${CLIENT_NAME}.req" \) -delete 2>/dev/null
    
    # Директория renewed (для обновленных сертификатов)
    rm -f "$DIR_PKI/renewed/issued/${CLIENT_NAME}.crt" 2>/dev/null
    rm -f "$DIR_PKI/renewed/private/${CLIENT_NAME}.key" 2>/dev/null
    rm -f "$DIR_PKI/renewed/reqs/${CLIENT_NAME}.req" 2>/dev/null
    
    # Директория inline
    rm -f "$DIR_PKI/inline/${CLIENT_NAME}.inline" 2>/dev/null
    
    # Директория revoked (отозванные сертификаты)
    rm -f "$DIR_PKI/revoked/issued/${CLIENT_NAME}.crt" 2>/dev/null
    rm -f "$DIR_PKI/revoked/private/${CLIENT_NAME}.key" 2>/dev/null

    # 3. Удаляем сертификаты по серийному номеру из всех мест
    if [ -n "$CERT_SERIAL" ]; then
        echo "Удаляем сертификаты по серийному номеру ${CERT_SERIAL}..."
        rm -f "$DIR_PKI/certs_by_serial/${CERT_SERIAL}.pem" 2>/dev/null
        rm -f "$DIR_PKI/revoked/certs_by_serial/${CERT_SERIAL}.crt" 2>/dev/null
        rm -f "$DIR_PKI/revoked/certs_by_serial/${CERT_SERIAL}.pem" 2>/dev/null
        rm -f "$DIR_PKI/newcerts/${CERT_SERIAL}.pem" 2>/dev/null
    fi

    # 4. Удаляем запись из index.txt
    echo "Корректируем базу данных (index.txt)..."
    
    # Если есть серийный номер - удаляем по нему
    if [ -n "$CERT_SERIAL" ]; then
        sed -i'.bak' "/${CERT_SERIAL}/d" "$INDEX"
    else
        # Если серийного номера нет, удаляем по имени клиента
        sed -i'.bak' "/\/CN=${CLIENT_NAME}\//d" "$INDEX"
    fi
    
    echo "БД скорректирована."

    # 5. Обновляем список отзыва сертификатов (CRL)
    echo 'Создаем новый список отзыва сертификатов (CRL)...'
    $EASY_RSA/easyrsa gen-crl 2>/dev/null
    chmod +r $DIR_PKI/crl.pem 2>/dev/null

    echo 'Удаление завершено!
Если вы хотите отключить пользователя, перезапустите службу с помощью команды: docker-compose restart openvpn.'
}

listOpenVPN(){
    [[ -n "$CLIENT_NAME" ]] && return
    echo
    echo 'OpenVPN client names:'
    
    # Проходим по всем файлам сертификатов
    for cert_file in "$DIR_PKI/issued"/*.crt; do
        [ -e "$cert_file" ] || continue
        
        # Извлекаем имя клиента из имени файла
        client_name=$(basename "$cert_file" .crt)
        
        # Пропускаем служебные сертификаты (CA, сервер и т.д.)
        [[ "$client_name" == "ca" ]] && continue
        [[ "$client_name" == "server" ]] && continue
        [[ "$client_name" == "antizapret-server" ]] && continue
        
        # Проверяем, есть ли соответствующий .ovpn файл (значит клиент активен)
        if [[ -f "$DIR_OPENVPN/clients/${client_name}.ovpn" ]]; then
            # Получаем дату окончания сертификата
            expire_date=$(openssl x509 -enddate -noout -in "$cert_file" 2>/dev/null | cut -d= -f2)
            
            # Конвертируем в формат DD-MM-YYYY для удобства парсинга в Python
            if [ -n "$expire_date" ]; then
                expire_formatted=$(date -d "$expire_date" +"%d-%m-%Y" 2>/dev/null || echo "unknown")
                # Вывод в формате: Имя_клиента|Дата_окончания
                echo "${client_name}|${expire_formatted}"
            else
                echo "${client_name}|unknown"
            fi
        fi
    done | sort
}

listWireGuard(){
    [[ -n "$CLIENT_NAME" ]] && return
    echo
    echo 'WireGuard/AmneziaWG client names:'
    
    # Путь к конфигам WireGuard (проверьте актуальность пути на вашем сервере)
    WG_DIR="/etc/wireguard"
    
    # Ищем файлы конфига клиентов (обычно wg0.conf или в подпапках)
    # В данном примере ищем файлы .conf, исключая основной серверный конфиг
    find "$WG_DIR" -name "*.conf" -type f 2>/dev/null | while read -r conf_file; do
        filename=$(basename "$conf_file")
        # Исключаем основные конфиги сервера
        [[ "$filename" == "wg0.conf" ]] && continue
        [[ "$filename" == "server.conf" ]] && continue
        
        # Извлекаем имя клиента из имени файла (например, client-name.conf -> client-name)
        client_name="${filename%.conf}"
        echo "$client_name"
    done | sort
}


if ! [[ "$OPTION" =~ ^[1-6]$ ]]; then
	echo
	echo 'Please choose option:'
	echo '    1) OpenVPN - Добавление/Обновление сертификата клиента'
	echo '    2) OpenVPN - Удаление клиента'
	echo '    3) OpenVPN - список клиентов'
	echo '    6) WireGuard - список клиентов'
	until [[ "$OPTION" =~ ^[1-6]$ ]]; do
		read -rp 'Option choice [1-6]: ' -e OPTION
	done
fi

case "$OPTION" in
	1)
		echo "OpenVPN - Добавление/Обновление сертификата клиента $CLIENT_NAME $CLIENT_CERT_EXPIRE"
		askClientName
		addOpenVPN
		;;
	2)
		echo "OpenVPN - Удаление клиента $CLIENT_NAME"
		listOpenVPN
		askClientName
		deleteOpenVPN
		;;
	3)
		echo 'OpenVPN - List clients'
		listOpenVPN
		;;
	6)
		echo 'WireGuard - List clients'
		listWireGuard
		;;
esac
exit 0