#!/bin/bash
#
# Проверка соответствия сертификатов и файлов конфигурации
#
# chmod +x check-client.sh && ./check-client.sh
#

set -e

handle_error() {
    echo "$(lsb_release -ds) $(uname -r) $(date --iso-8601=seconds)"
    echo -e "\e[1;31mError at line $1: $2\e[0m"
    exit 1
}

trap 'handle_error $LINENO "$BASH_COMMAND"' ERR

# Переменные (как в client.sh)
DIR_OPENVPN=/root/web/openvpn
DIR_PKI=$DIR_OPENVPN/pki
INDEX="$DIR_PKI/index.txt"
export LC_ALL=C
export EASYRSA_PKI=$DIR_PKI
EASY_RSA=/usr/share/easy-rsa

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функция проверки имени на наличие "server"
isServerFile() {
    local name="$1"
    if [[ "$name" =~ server ]]; then
        return 0  # true - это серверный файл
    else
        return 1  # false - это клиентский файл
    fi
}

echo -e "${GREEN}=== Проверка соответствия сертификатов и .ovpn файлов ===${NC}"
echo

# Массивы для хранения имен
declare -a OVPN_CLIENTS
declare -a PKI_CERTS
declare -a SERIAL_CERTS
declare -a ORPHANED_CERTS
declare -a ORPHANED_SERIALS
declare -a MISSING_OVPN

# 1. Получаем список всех .ovpn файлов (без расширения)
echo "Сканирование директории: $DIR_OPENVPN/clients/"
if [[ -d "$DIR_OPENVPN/clients" ]]; then
    while IFS= read -r -d '' file; do
        basename_file=$(basename "$file" .ovpn)
        # ИСКЛЮЧАЕМ файлы с "server" в названии
        if ! isServerFile "$basename_file"; then
            OVPN_CLIENTS+=("$basename_file")
        else
            echo -e "  ${BLUE}Пропущено (server): ${basename_file}.ovpn${NC}"
        fi
    done < <(find "$DIR_OPENVPN/clients" -maxdepth 1 -name "*.ovpn" -type f -print0 2>/dev/null)
else
    echo -e "${RED}Директория $DIR_OPENVPN/clients не найдена!${NC}"
fi

# 2. Получаем список всех сертификатов в issued/
echo "Сканирование директории: $DIR_PKI/issued/"
if [[ -d "$DIR_PKI/issued" ]]; then
    while IFS= read -r -d '' file; do
        basename_file=$(basename "$file" .crt)
        # Исключаем ca.crt и файлы с "server" в названии
        if [[ "$basename_file" != "ca" ]] && ! isServerFile "$basename_file"; then
            PKI_CERTS+=("$basename_file")
        elif isServerFile "$basename_file"; then
            echo -e "  ${BLUE}Пропущено (server): ${basename_file}.crt${NC}"
        fi
    done < <(find "$DIR_PKI/issued" -maxdepth 1 -name "*.crt" -type f -print0 2>/dev/null)
else
    echo -e "${RED}Директория $DIR_PKI/issued не найдена!${NC}"
fi

# 3. Получаем список сертификатов по серийным номерам
echo "Сканирование директории: $DIR_PKI/certs_by_serial/"
if [[ -d "$DIR_PKI/certs_by_serial" ]]; then
    while IFS= read -r -d '' file; do
        basename_file=$(basename "$file" .pem)
        # Проверяем CN сертификата - если содержит "server", пропускаем
        CERT_CN=$(openssl x509 -in "$file" -noout -subject 2>/dev/null | sed -n 's/.*CN\s*=\s*\([^,/]*\).*/\1/p' || echo "")
        if ! isServerFile "$CERT_CN"; then
            SERIAL_CERTS+=("$basename_file")
        else
            echo -e "  ${BLUE}Пропущено (server CN: ${CERT_CN}): ${basename_file}.pem${NC}"
        fi
    done < <(find "$DIR_PKI/certs_by_serial" -maxdepth 1 -name "*.pem" -type f -print0 2>/dev/null)
else
    echo -e "${YELLOW}Директория $DIR_PKI/certs_by_serial не найдена (это нормально для новой установки)${NC}"
fi

echo
echo -e "${YELLOW}Результаты проверки:${NC}"
echo "----------------------------------------"
echo "Найдено .ovpn файлов (клиенты): ${#OVPN_CLIENTS[@]}"
echo "Найдено сертификатов (issued, клиенты): ${#PKI_CERTS[@]}"
echo "Найдено сертификатов (by serial, клиенты): ${#SERIAL_CERTS[@]}"
echo "----------------------------------------"
echo

# 4. Проверяем сертификаты без .ovpn файлов (issued/)
echo -e "${RED}1. Сертификаты без соответствующих .ovpn файлов (issued/):${NC}"
ORPHANED_COUNT=0

for cert in "${PKI_CERTS[@]}"; do
    # Дополнительная проверка на "server"
    if isServerFile "$cert"; then
        continue
    fi
    
    found=0
    for ovpn in "${OVPN_CLIENTS[@]}"; do
        if [[ "$cert" == "$ovpn" ]]; then
            found=1
            break
        fi
    done
    
    if [[ $found -eq 0 ]]; then
        # Проверяем статус сертификата в index.txt
        STATUS=""
        if [[ -f "$INDEX" ]]; then
            STATUS=$(grep "/CN=$cert/" "$INDEX" 2>/dev/null | head -1 | awk '{print $1}' || echo "")
        fi
        
        STATUS_LABEL="неизвестно"
        if [[ "$STATUS" == "V" ]]; then
            STATUS_LABEL="${GREEN}действующий${NC}"
        elif [[ "$STATUS" == "R" ]]; then
            STATUS_LABEL="${YELLOW}отозван${NC}"
        elif [[ "$STATUS" == "E" ]]; then
            STATUS_LABEL="${RED}истек${NC}"
        fi
        
        echo -e "  ❌ ${cert}.crt (статус: $STATUS_LABEL)"
        ORPHANED_CERTS+=("$cert")
        ((ORPHANED_COUNT++)) || true
    fi
done

if [[ $ORPHANED_COUNT -eq 0 ]]; then
    echo -e "  ${GREEN}✓ Все сертификаты имеют соответствующие .ovpn файлы${NC}"
fi

echo
echo "----------------------------------------"

# 5. Проверяем сертификаты по серийным номерам без .ovpn файлов
echo -e "${BLUE}2. Сертификаты по серийному номеру без .ovpn файлов (certs_by_serial/):${NC}"
ORPHANED_SERIAL_COUNT=0

for serial in "${SERIAL_CERTS[@]}"; do
    # Получаем информацию о сертификате из файла
    CERT_INFO=""
    CERT_CN=""
    CERT_STATUS=""
    
    if [[ -f "$DIR_PKI/certs_by_serial/${serial}.pem" ]]; then
        # Извлекаем CN из сертификата
        CERT_CN=$(openssl x509 -in "$DIR_PKI/certs_by_serial/${serial}.pem" -noout -subject 2>/dev/null | sed -n 's/.*CN\s*=\s*\([^,/]*\).*/\1/p' || echo "")
        
        # Пропускаем серверные сертификаты
        if isServerFile "$CERT_CN"; then
            continue
        fi
        
        # Проверяем статус в index.txt по серийному номеру
        if [[ -f "$INDEX" ]]; then
            CERT_STATUS=$(grep "$serial" "$INDEX" 2>/dev/null | head -1 | awk '{print $1}' || echo "")
        fi
    fi
    
    # Проверяем, есть ли .ovpn файл для этого сертификата
    found=0
    if [[ -n "$CERT_CN" ]]; then
        for ovpn in "${OVPN_CLIENTS[@]}"; do
            if [[ "$CERT_CN" == "$ovpn" ]]; then
                found=1
                break
            fi
        done
    fi
    
    if [[ $found -eq 0 ]]; then
        STATUS_LABEL="неизвестно"
        if [[ "$CERT_STATUS" == "V" ]]; then
            STATUS_LABEL="${GREEN}действующий${NC}"
        elif [[ "$CERT_STATUS" == "R" ]]; then
            STATUS_LABEL="${YELLOW}отозван${NC}"
        elif [[ "$CERT_STATUS" == "E" ]]; then
            STATUS_LABEL="${RED}истек${NC}"
        fi
        
        CN_DISPLAY="${CERT_CN:-неизвестно}"
        echo -e "  ❌ ${serial}.pem (CN: ${CN_DISPLAY}, статус: $STATUS_LABEL)"
        ORPHANED_SERIALS+=("$serial")
        ((ORPHANED_SERIAL_COUNT++)) || true
    fi
done

if [[ $ORPHANED_SERIAL_COUNT -eq 0 ]]; then
    echo -e "  ${GREEN}✓ Все сертификаты by serial имеют соответствующие .ovpn файлы${NC}"
fi

echo
echo "----------------------------------------"

# 6. Проверяем .ovpn файлы без сертификатов
echo -e "${YELLOW}3. .ovpn файлы без соответствующих сертификатов:${NC}"
MISSING_COUNT=0

for ovpn in "${OVPN_CLIENTS[@]}"; do
    # Дополнительная проверка на "server"
    if isServerFile "$ovpn"; then
        continue
    fi
    
    found=0
    for cert in "${PKI_CERTS[@]}"; do
        if [[ "$ovpn" == "$cert" ]]; then
            found=1
            break
        fi
    done
    
    if [[ $found -eq 0 ]]; then
        echo -e "  ⚠️  ${ovpn}.ovpn (сертификат не найден в PKI/issued/)"
        MISSING_OVPN+=("$ovpn")
        ((MISSING_COUNT++)) || true
    fi
done

if [[ $MISSING_COUNT -eq 0 ]]; then
    echo -e "  ${GREEN}✓ Все .ovpn файлы имеют соответствующие сертификаты${NC}"
fi

echo
echo "========================================"

# 7. Итоговая статистика
echo -e "${GREEN}ИТОГО:${NC}"
echo "  Сертификатов (issued) без .ovpn: $ORPHANED_COUNT"
echo "  Сертификатов (by serial) без .ovpn: $ORPHANED_SERIAL_COUNT"
echo "  .ovpn файлов без сертификатов: $MISSING_COUNT"
echo "========================================"

# 8. Рекомендации и команды для удаления
if [[ $ORPHANED_COUNT -gt 0 || $ORPHANED_SERIAL_COUNT -gt 0 ]]; then
    echo
    echo -e "${YELLOW}Рекомендации:${NC}"
    echo "  • Проверьте, были ли эти сертификаты удалены намеренно"
    echo "  • Если сертификаты не нужны - удалите их через:"
    echo "    ./client.sh 2 <имя_клиента>"
    echo
    echo -e "${RED}Файлы, которые можно удалить вручную:${NC}"
    
    if [[ $ORPHANED_COUNT -gt 0 ]]; then
        echo
        echo "  # Сертификаты из issued/:"
        for cert in "${ORPHANED_CERTS[@]}"; do
            # Финальная проверка на "server" перед выводом команды
            if ! isServerFile "$cert"; then
                echo "  rm -f $DIR_PKI/issued/${cert}.crt"
                echo "  rm -f $DIR_PKI/private/${cert}.key"
                echo "  rm -f $DIR_PKI/reqs/${cert}.req"
            fi
        done
    fi
    
    if [[ $ORPHANED_SERIAL_COUNT -gt 0 ]]; then
        echo
        echo "  # Сертификаты из certs_by_serial/:"
        for serial in "${ORPHANED_SERIALS[@]}"; do
            echo "  rm -f $DIR_PKI/certs_by_serial/${serial}.pem"
        done
        echo
        echo "  # Также проверьте директорию revoked/certs_by_serial/:"
        for serial in "${ORPHANED_SERIALS[@]}"; do
            echo "  rm -f $DIR_PKI/revoked/certs_by_serial/${serial}.crt 2>/dev/null"
            echo "  rm -f $DIR_PKI/revoked/certs_by_serial/${serial}.pem 2>/dev/null"
        done
    fi
    
    echo
    echo -e "${YELLOW}После удаления файлов не забудьте обновить CRL:${NC}"
    echo "  $EASY_RSA/easyrsa gen-crl"
fi

if [[ $MISSING_COUNT -gt 0 ]]; then
    echo
    echo -e "${YELLOW}Внимание:${NC}"
    echo "  • Эти .ovpn файлы могут быть нерабочими"
    echo "  • Рекомендуется перегенерировать сертификаты:"
    echo "    ./client.sh 1 <имя_клиента>"
fi

echo
exit 0