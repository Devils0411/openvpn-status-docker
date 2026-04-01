#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
ADMIN_USER=${ADMIN_USERNAME:-"admin"}
ADMIN_PASS=${ADMIN_PASSWORD:-"admin"}

ADMIN_PASS=$(PYTHONIOENCODING=utf-8 python3 -c "from main import add_user; add_user('$ADMIN_USER', 'admin', '$ADMIN_PASS')")
echo -e "Добавлен пользователь: \e[32m$ADMIN_USER\e[0m с паролем: \e[32m$ADMIN_PASS\e[0m"
