#!/usr/bin/env bash
#Процесс инициализации
scripts/init.sh
#Задаем нового пользователя
scripts/add_user.sh $ADMIN_USERNAME $ADMIN_PASSWORD
# Запускаем supervisord
exec supervisord -c /etc/supervisord.conf