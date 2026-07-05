import logging
import mysql.connector
from mysql.connector import Error
from telebot import TeleBot, types, logger
from telebot.util import content_type_media, content_type_service

import os
import re
import requests
import urllib.parse
from flask import Flask, request

# Для нечеткого поиска (нужно установить: pip install thefuzz)
try:
    from thefuzz import process
except ImportError:
    process = None
    print("Внимание: библиотека thefuzz не установлена. Нечеткий поиск работать не будет.")

start_text = """
<b>Приветствуем! Это бот-мартиролог мемориала жертв политических репрессий 12 километр в Екатеринбурге.</b>

<u><b>Что умеет этот бот?</b></u>

Бот содержит полный список захороненных в Мемориальном комплексе 12 километр жертв Большого Террора. С помощью поискового запроса вы можете получить карточку репрессированного. 
Также вы можете изучить схему мемориального комплекса и найти нужную стелу. Для этого нажмите кнопку "🗺 Схема комплекса"

<u><b>Как использовать бот?</b></u>

Введите фамилию репрессированного и получите карточки всех репрессированных с такой фамилией.

<u><b>Какая информация есть в карточке?</b></u>

Карточка репрессированного содержит:
1. Фамилию и инициалы
2. Год рождения
3. Год расстрела
4. Номер сектора 
5. Номер стелы и схема сектора с указанием номеров стелы
6. Номер и фото плиты 
7. Приблизительные координаты плиты
"""

no_stele_text = """
К сожалению, эта плита ещё не установлена на стелу. Увы, проект расширения мемориала до сих пор не осуществлён и на данный момент заморожен. Однако, плита отлита и мы надеемся, что она будет установлена на своё место.
Тем не менее есть фото плиты и этот человек есть в списках репрессированных и упомянут в Книге Памяти
"""

# Обновленное описание с упоминанием поиска по годам
help_text = """<b>Как использовать бот?</b>

Введите фамилию репрессированного и получите карточки всех репрессированных с такой фамилией.
Вы можете ввести как полную фамилию, так и фамилию с инициалами (например, "Иванов И.И.").

🔍 <b>Расширенный поиск:</b>
Вы можете добавить год рождения или год расстрела для более точного поиска (например, "Иванов 1890" или "Петров 1937").

Если вы не уверены в написании фамилии, просто введите ее, и бот попытается найти похожие варианты.
"""

app = Flask(__name__)


def env_bool(name: str, default: bool = False) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


IS_DEV = env_bool("IS_DEV", default=True)

if IS_DEV:
    env_file = ".env_local"
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())

POLLING_MODE = env_bool("POLLING_MODE", default=IS_DEV)

TOKEN = os.environ.get("TOKEN")
bot = TeleBot(TOKEN)
logger.setLevel(logging.DEBUG)
url = os.environ.get("WEBHOOK_URL", "https://martirolog-89a3aa406540.herokuapp.com/")
img_url = "http://46.101.97.212:8090/martirolog_new/"
map_url = img_url + "karty/sector-all.png"

conn = {
    "host": os.environ.get("DATABASE_HOST") or "127.0.0.1",
    "port": int(os.environ.get("DATABASE_PORT") or 3306),
    "user": os.environ.get("DATABASE_USER") or "root",
    "password": os.environ.get("DATABASE_PASSWORD"),
}
db_name = os.environ.get("DATABASE_NAME")
table_name = os.environ.get("TABLE_NAME")


def find_by_name(search_query):
    results = []
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**conn)
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)

            # Извлекаем год (4 цифры) из запроса пользователя
            years = re.findall(r'\b\d{4}\b', search_query)
            # Удаляем год из строки, чтобы получить чистое ФИО
            name_part = re.sub(r'\b\d{4}\b', '', search_query).strip()

            # Если пользователь ввел только год, оставляем его как есть
            if not name_part and years:
                name_part = search_query

            query = f"SELECT * FROM {db_name}.{table_name} WHERE LOWER(FIO) LIKE %s"
            params = [f"{name_part.lower()}%"]

            # Если найден год, добавляем условие поиска по году рождения или расстрела
            if years:
                year = years[0]  # Берем первый найденный год
                query += " AND (CAST(BIRTH_YEAR AS CHAR) LIKE %s OR CAST(EXECUTION_YEAR AS CHAR) LIKE %s)"
                params.extend([f"%{year}%", f"%{year}%"])

            query += " ORDER BY FIO"
            cursor.execute(query, tuple(params))
            results = cursor.fetchall()
    except Error as e:
        logger.error(f"Database error in find_by_name: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
    return results


def get_fuzzy_suggestion(name):
    if not process:
        return None

    all_names = []
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**conn)
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            cursor.execute(f"SELECT FIO FROM {db_name}.{table_name}")
            all_names = [row['FIO'] for row in cursor.fetchall()]

            best_match = process.extractOne(name, all_names)
            if best_match and best_match[1] > 75:
                return best_match[0]
    except Error as e:
        logger.error(f"Database error in get_fuzzy_suggestion: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
    return None


@bot.message_handler(commands=['start'])
def start(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btn_map = types.KeyboardButton(text='🗺 Схема комплекса')
    btn_help = types.KeyboardButton(text='ℹ️ Как использовать бот?')
    keyboard.add(btn_map, btn_help)
    bot.send_message(message.chat.id, start_text, parse_mode='html', reply_markup=keyboard)


@bot.message_handler(func=lambda message: message.text in ['Схема комплекса', '🗺 Схема комплекса'])
def send_schema(message: types.Message):
    try:
        # Скачиваем картинку сами, так как Telegram не любит прямые http:// ссылки
        response = requests.get(map_url, timeout=10)
        if response.status_code == 200:
            bot.send_photo(message.chat.id, response.content, caption="Схема мемориального комплекса")
        else:
            raise Exception(f"Status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to send map: {e}")
        # Запасной вариант, если фото не отправится
        bot.send_message(message.chat.id, text=f"<a href='{map_url}'>Схема комплекса</a>", parse_mode='html')


@bot.message_handler(func=lambda message: message.text in ['Как использовать бот?', 'ℹ️ Как использовать бот?'])
def send_help(message: types.Message):
    bot.send_message(message.chat.id, text=help_text, parse_mode='html')


@bot.message_handler(func=lambda message: True)
def send_person_details(message):
    try:
        fio = message.text
        if 'ФИО: ' in fio:
            fio = fio.split('ФИО: ')[-1]

        details = find_by_name(fio)

        if not details:
            # Для нечеткого поиска убираем годы из запроса, ищем только по фамилии
            name_only = re.sub(r'\b\d{4}\b', '', fio).strip() or fio
            suggestion = get_fuzzy_suggestion(name_only)
            if suggestion:
                response = f"Извините, мы никого не нашли с фамилией <b>{name_only}</b>.\n\nВозможно, вы имели в виду: <b>{suggestion}</b>?\n(Просто отправьте эту фамилию сообщением)"
            else:
                response = "Извините, мы никого не нашли с такой фамилией. Проверьте правильность написания."
            bot.send_message(message.chat.id, response, parse_mode='html')
            return

        # Если найдено больше 10 карточек, просим уточнить и останавливаем отправку
        if len(details) > 10:
            bot.send_message(
                message.chat.id,
                f"По вашему запросу найдено {len(details)} человек. Пожалуйста уточните запрос (например, добавьте инициалы, год рождения или год расстрела)."
            )
            return

        for detail in details:
            response_rows = [
                f"ФИО: <b>{detail.get('FIO', '-')}</b>",
                f"Год рождения: {detail.get('BIRTH_YEAR') or '-'}",
                f"Год расстрела: {detail.get('EXECUTION_YEAR') or '-'}",
            ]

            # В "Номер сектора" ссылка на Схему комплекса (map_url)
            if detail.get('SECTOR_NUMBER'):
                response_rows.append(
                    f"Номер сектора: {detail['SECTOR_NUMBER']} (<a href='{map_url}'>Схема комплекса</a>)")
            else:
                response_rows.append("Номер сектора: -")

            # В "Номер стелы" ссылка на Схему сектора (img_url + SECTOR_PHOTO)
            if detail.get('STELE_NUMBER'):
                sector_photo = detail.get('SECTOR_PHOTO', '')
                if sector_photo:
                    stele_link = f" (<a href='{img_url + sector_photo}'>Схема сектора</a>)"
                else:
                    stele_link = ""
                response_rows.append(f"Номер стелы: {detail['STELE_NUMBER']}{stele_link}")
            else:
                response_rows.append("Номер стелы: -")

            if detail.get('SLAB_PHOTO') and detail.get('SLAB_NUMBER'):
                response_rows.append(f"Номер плиты: {detail['SLAB_NUMBER']} (см. фото ниже)")
            elif detail.get('SLAB_PHOTO'):
                response_rows.append("Номер плиты: - (см. фото ниже)")
            else:
                response_rows.append("Номер плиты: -")

            if detail.get('STELE_COORD'):
                gmap_link = detail.get('STELE_GMAP_LINK', '#')
                response_rows.append(f"Координаты стелы: <a href='{gmap_link}'>{detail['STELE_COORD']}</a>")
            else:
                response_rows.append("Координаты стелы: -")

            if not detail.get('STELE_NUMBER'):
                response_rows.append(no_stele_text)

            text_response = "\n".join(response_rows)

            bot.send_message(
                message.chat.id,
                text_response,
                parse_mode='html',
                disable_web_page_preview=True
            )

            # Отправляем фото плиты напрямую в чат
            if detail.get('SLAB_PHOTO'):
                try:
                    raw_photo_url = img_url + detail['SLAB_PHOTO']
                    photo_url = urllib.parse.quote(raw_photo_url, safe=':/?=&')

                    img_response = requests.get(photo_url, timeout=10)
                    if img_response.status_code == 200:
                        bot.send_photo(
                            message.chat.id,
                            img_response.content,
                            caption=f"Плита: {detail.get('FIO', '')}"
                        )
                    else:
                        logger.error(f"Failed to download photo. Status: {img_response.status_code}, URL: {photo_url}")
                        bot.send_message(message.chat.id, f"Не удалось загрузить фото плиты. Ссылка: {photo_url}")
                except Exception as photo_err:
                    logger.error(f"Failed to send photo {raw_photo_url}: {photo_err}")
                    bot.send_message(message.chat.id, f"Не удалось загрузить фото плиты. Ссылка: {raw_photo_url}")

    except Exception as e:
        logger.error(f"Unexpected error in send_person_details for query '{fio}': {e}")
        bot.send_message(
            message.chat.id,
            "Произошла техническая ошибка. Мы уже работаем над ней. Пожалуйста, попробуйте позже."
        )


@bot.callback_query_handler(func=lambda call: True)
def unknown_callback(call) -> None:
    logger.info("Unknown callback %s", call.data)


@bot.message_handler(content_types=content_type_media + content_type_service)
def log_all(message) -> None:
    logger.info("Unknown content type %s", message.content_type)


def run_local():
    bot.delete_webhook()
    bot.infinity_polling(none_stop=True, timeout=60)


@app.route('/' + TOKEN, methods=['POST'])
def getMessage():
    bot.process_new_updates([types.Update.de_json(request.stream.read().decode("utf-8"))])
    return "!", 200


@app.route("/")
def webhook():
    bot.remove_webhook()
    bot.set_webhook(url=url + TOKEN)
    return "!", 200


if __name__ == "__main__":
    if POLLING_MODE:
        run_local()
    else:
        app.run(threaded=True, host="0.0.0.0", port=int(os.environ.get('PORT', 5000)))