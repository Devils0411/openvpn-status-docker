#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
ADMIN_USER=${ADMIN_USERNAME:-"admin"}
ADMIN_PASS=${ADMIN_PASSWORD:-"admin"}

ADMIN_PASS=$(PYTHONIOENCODING=utf-8 python3 -c "from main import change_admin_password_2; change_admin_password_2('$ADMIN_PASS')")
echo -e "Пароль успешно заменен на: \e[32m$ADMIN_PASS\e[0m"