# OpenVPN-Status in Docker + TelegramBot

Идея данного репозитория - собрать проект [StatusOpenVPN + TelegramBot](https://github.com/TheMurmabis/StatusOpenVPN), в контейнер Docker'а.


<details>
  <summary>Содержание</summary>
  
- [Отличия](#Отличия)  
- [Требования](#Требования)
- [Установка контейнера](#Установка)
  - [Варианты запуска](#Варианты-запуска)
- [Обновление](#Обновление)
- [Смена пароля администратора](#Cмена-пароля)
- [Включение HTTPS](#HTTPS)
- [Настройка Telegram-бота](#Создание-и-настройка-бота)
</details>

# Отличия
- Отключено сбор/отображение статистики WireGuard *(в разработке включить данную функцию)*
- Включено логирование ошибок в папку `/root/openvpn-status/src/data/logs`
- Сбор сетевой статистики vnstat ведется только по интерфейсам `eth|ens`
- Отображение текущей сетевой нагрузки только по интерфейсам `eth|ens`
- Изменения касаемые Telegram-бота
  - Добавлено отображение текущей сетевой нагрузки в данный момент.
  - В списке профилей добавлено отображения срока действия сертификата.
  - Добавлена возможность выбора пользователя `telegramID` из списка контактов для привязки к профилю opvn.
  - Добавлена возможность привязки 1 пользователя `telegramID` к нескольким профилям opvn.

# Требования

Для успешной установки необходимо, чтобы на сервере были установлены следующие компоненты:
- [antizapret-vpn-docker](https://github.com/xtrime-ru/antizapret-vpn-docker) *(обязательно)*.
- Минимальный объем ОЗУ - 140Мб. С Telegram Bot - 350Мб

# Установка

1) Клонировать репозиторий:
   ```bash
   git clone https://github.com/Devils0411/openvpn-status-docker.git openvpn-status
   cd openvpn-status
   ```
2) Создать docker-compose.override.yml с указанием используемого порта, логина и пароля.
```yml
services:
  openvpn-status:
    environment:
      - PORT=2000
      - ADMIN_USERNAME=admin!
      - ADMIN_PASSWORD=admin!
```

3) Запуск контейнера:
```shell
   docker compose up -d
   docker system prune -f
```

## Варианты запуска
Есть несколько вариантов запуска контейнеров:

- [Запуск и подключение к сети antizapret](./docker-compose.override.az.yml)
- [Запуск и подключение к сети host](./docker-compose.override.host.yml)
- [Классический запуск](./docker-compose.override.sample.yml)

# Обновление
```shell
git pull
docker compose down --remove-orphans
docker compose up -d --remove-orphans
```

# Cмена пароля
Чтобы сменить пароль администратора, запустите следующую команду:
```bash
docker exec -e ADMIN_PASSWORD='admin!' openvpn-status ./scripts/chg_pwd.sh
```

# HTTPS
## ⚠ **Внимание!** HTTPS работает в своей сети, или сети HOST.
Включение HTTPS доступно через переменную `HTTPS_ON=Y`. Пример включения HTTPS в сети `host` через `docker-compose.override.yml`
Если домен не указан - будет создан самоподписанный сертификат.
```yml
services:
  openvpn-status:
    network_mode: host
    extends:
      file: docker-compose.yml
      service: openvpn-status
    environment:
      - PORT=2000
      - ADMIN_USERNAME=admin!
      - ADMIN_PASSWORD=admin!
      - HTTPS_ON=Y #Включение HTTPS
#      - DOMAIN_NAME=Test.ru
```

# Создание и настройка бота

### ⚠ **Внимание!** Телеграмм бот реализует функции скрипта [client.sh](./scripts/client.sh)

**- Открываем BotFather**
- Откройте Telegram и найдите [**`@BotFather`**](https://t.me/BotFather).
- Перейдите в чат с BotFather и нажмите **"Start"**.

**- Создание нового бота**
- Отправьте команду:
   ```  
   /newbot  
   ```  
- BotFather попросит ввести **имя бота** (**Name**), например: 
   ```  
   OpenVPN Bot  
   ```  
- Затем потребуется указать **username бота** (он должен заканчиваться на `bot`), например:  
   ```  
   OpenVPN_bot  
   ```  
- После успешного создания BotFather отправит **токен API**, который выглядит так:  
   ```  
   1234567890:ABCDEFGHIJKLMNOPQRSTUVWXYZ12345678  
   ```

### Получение личного ID**  

1. Открываем Telegram и ищем [**`@userinfobot`**](https://t.me/userinfobot).  
2. Переходим в чат и нажимаем **"Start"**.  
3. В ответ получаем сообщение:
```
Id: 123456789
```

### Добавление в docker-compose.override.yml

Записываем полученные TOKEN и ID в `docker-compose.override.yml`
```yml
services:
  openvpn-status:
    environment:
      - BOT_ON=Y
      - BOT_TOKEN=1234567890:ABCDEFGHIJKLMNOPQRSTUVWXYZ12345678
      - ADMIN_ID=123456789
```

## Примечания

- Если используете сеть `host`, убедитесь, что ваш сервер имеет открытый порт, на котором будет работать ваше приложение.
- Данные для OpenVPN считываются из файлов `openvpn-status.log` из директории хоста `/root/antizapret/config/openvpn/log`. Проверьте конфиг сервера `status-version` должен быть версии `2`
# OpenVPN-Status in Docker + TelegramBot
- Если используете сеть `host`, убедитесь, что ваш сервер имеет открытый порт, на котором будет работать ваше приложение.
- Данные для OpenVPN считываются из файлов `openvpn-status.log` из директории хоста `/root/antizapret/config/openvpn/log`. Проверьте конфиг сервера `status-version` должен быть версии `2`
