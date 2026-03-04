# <h1 align="center" >OpenVPN-Status in Docker + TelegramBot</h1>

## Описание
Идея данного репозитория - собрать проект [StatusOpenVPN + TelegramBot] (https://github.com/TheMurmabis/StatusOpenVPN), в контейнер Docker'а

<details>
  <summary>Содержание</summary>
  
- [Требования](#Требования)
- [Установка контейнера](#Установка)
- [Обновление](#Обновление)
- [Смена пароля администратора](#Cмена-пароля)
- [Настройка Telegram-бота](#Создание и настройка бота)

</details>

# Установка и настройка 

## Требования

Для успешной установки необходимо, чтобы на сервере были установлены следующие компоненты:

- [antizapret-vpn-docker](https://github.com/xtrime-ru/antizapret-vpn-docker) *(обязательно)*.
- Минимальный объем ОЗУ - 140Мб. С Telegram Bot - 350Мб

## Установка

1. Клонировать репозиторий:
   ```bash
   git clone https://github.com/Devils0411/openvpn-status-docker.git openvpn-status
   cd openvpn-status
   ```
2. Создать docker-compose.override.yml с указанием используемого порта, логина и пароля.
```yml
services:
  openvpn-status:
    environment:
      - PORT=2000
      - ADMIN_USERNAME=admin!
      - ADMIN_PASSWORD=admin!
```
Есть несколько вариантов запуска контейнеров:

- [Запуск и подключение к сети antizapret](./docker-compose.override.az.yml)
- [Запуск и подключение к сети host](./docker-compose.override.host.yml)
- [Классический запуск](./docker-compose.override.sample.yml)

3. Запуск контейнера:
```shell
   docker compose up -d
   docker system prune -f
```

## Обновление
```shell
git pull
docker compose down --remove-orphans
docker compose up -d --remove-orphans
```

## Cмена-пароля
Чтобы сменить пароль администратора, запустите пустите следующую команду:
```bash
docker exec -e ADMIN_PASSWORD='admin!' openvpn-status ./scripts/chg_pwd.sh
```

## Создание и настройка бота

### ⚠ **Внимание!** Телеграмм бот реализует функции скрипта [client.sh](./scripts/client.sh)

### 1.Создание бота



## Примечания

- Если используете сеть `host`, убедитесь, что ваш сервер имеет открытый порт, на котором будет работать ваше приложение.
3. Данные для OpenVPN считываются из файлов `openvpn-status.log` из директории хоста `/root/antizapret/config/openvpn/log`.
