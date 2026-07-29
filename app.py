import html
import logging
import os
import re
import time
import urllib.parse

import mysql.connector
import requests
from flask import Flask, request
from mysql.connector import Error
from telebot import TeleBot, types, logger
from telebot.util import content_type_media, content_type_service

# Нечёткий поиск (подсказки при опечатках): pip install rapidfuzz
try:
    from rapidfuzz import process as fuzz_process
    from rapidfuzz import utils as fuzz_utils
except ImportError:
    fuzz_process = None
    fuzz_utils = None
    print("Внимание: библиотека rapidfuzz не установлена. Подсказки при опечатках работать не будут.")

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

help_text = """<b>Как использовать бот?</b>

Введите фамилию репрессированного и получите карточки всех репрессированных с такой фамилией.
Вы можете ввести как полную фамилию, так и фамилию с инициалами (например, "Иванов И.И.").

🔍 <b>Уточнение по году:</b>
Если найдено слишком много людей, добавьте к фамилии год рождения или год расстрела (например, "Иванов 1890" или "Петров 1937"). Поиск всегда начинается с фамилии — искать только по году нельзя.

Если вы не уверены в написании фамилии, просто введите её, и бот предложит похожие варианты.
"""

enter_surname_text = "Пожалуйста, введите фамилию. Поиск по одному только году не поддерживается — год можно добавить к фамилии для уточнения (например, «Иванов 1937»)."

not_text_reply = "Бот понимает только текстовые сообщения. Введите фамилию репрессированного — например: Иванов"

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
if not TOKEN:
    raise SystemExit("Ошибка: переменная окружения TOKEN не задана (токен бота).")

bot = TeleBot(TOKEN)
logger.setLevel(logging.DEBUG if IS_DEV else logging.INFO)
url = os.environ.get("WEBHOOK_URL", "")
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

MAX_QUERY_LEN = 100        # максимальная длина запроса пользователя
MAX_CARDS = 10             # больше этого — показываем компактный список
MAX_LIST_LINES = 30        # больше этого — просим уточнить запрос
FUZZY_SUGGESTIONS = 3      # сколько подсказок-кнопок предлагать
FIO_CACHE_TTL = 24 * 3600  # раз в сутки перечитываем список ФИО из базы

esc = html.escape


def db_query(query, params=()):
    """Выполняет запрос к базе. Возвращает список строк или None при ошибке."""
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**conn)
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params)
        return cursor.fetchall()
    except Error as e:
        logger.error(f"Database error: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def parse_query(text):
    """Разбирает запрос пользователя на фамилию и год (год — только уточнение)."""
    text = text.strip()[:MAX_QUERY_LEN]
    # наследие старого формата: если переслали строку карточки "ФИО: ..."
    if "ФИО: " in text:
        text = text.split("ФИО: ")[-1]
    years = re.findall(r"\b\d{4}\b", text)
    name = re.sub(r"\b\d{4}\b", "", text)
    # убираем спецсимволы LIKE (% и _), чтобы ими нельзя было управлять поиском
    name = name.replace("%", "").replace("_", "")
    name = re.sub(r"\s+", " ", name).strip()
    # фамилия обязана содержать хотя бы одну букву
    if not re.search(r"[а-яёa-z]", name, re.IGNORECASE):
        name = ""
    return name, (years[0] if years else None)


def normalize_fio(s):
    """Нижний регистр и ё→е, чтобы «Семенов» находил «СЕМЁНОВ»."""
    return s.lower().replace("ё", "е")


def find_by_name(name, year=None):
    """Ищет людей по началу ФИО, год — дополнительный фильтр."""
    query = (
        f"SELECT * FROM {db_name}.{table_name} "
        "WHERE REPLACE(LOWER(FIO), 'ё', 'е') LIKE %s"
    )
    params = [normalize_fio(name) + "%"]
    if year:
        query += " AND (CAST(BIRTH_YEAR AS CHAR) = %s OR CAST(EXECUTION_YEAR AS CHAR) = %s)"
        params.extend([year, year])
    query += " ORDER BY FIO"
    return db_query(query, tuple(params))


# Кеш всех ФИО в памяти — чтобы подсказки при опечатках не читали базу каждый раз
_fio_cache = {"names": [], "ts": 0.0}


def get_all_fio():
    now = time.time()
    if _fio_cache["names"] and now - _fio_cache["ts"] < FIO_CACHE_TTL:
        return _fio_cache["names"]
    rows = db_query(f"SELECT DISTINCT FIO FROM {db_name}.{table_name}")
    if rows:
        _fio_cache["names"] = [r["FIO"] for r in rows]
        _fio_cache["ts"] = now
    return _fio_cache["names"]


def get_fuzzy_suggestions(name, limit=FUZZY_SUGGESTIONS):
    """Возвращает до `limit` похожих ФИО (подсказки при опечатке)."""
    if not fuzz_process:
        return []
    names = get_all_fio()
    if not names:
        return []
    # processor приводит строки к одному виду (регистр и т.п.) — как делала thefuzz
    matches = fuzz_process.extract(
        name, names, limit=limit * 5, score_cutoff=70,
        processor=fuzz_utils.default_process,
    )
    suggestions = []
    for match in matches:
        candidate = match[0]
        # среди тёзок предлагаем каждую фамилию один раз
        if candidate not in suggestions:
            suggestions.append(candidate)
        if len(suggestions) >= limit:
            break
    return suggestions


def suggestion_keyboard(names):
    keyboard = types.InlineKeyboardMarkup()
    for name in names:
        # ограничение Telegram: callback_data не длиннее 64 байт
        data = ("s:" + name).encode("utf-8")[:64].decode("utf-8", errors="ignore")
        keyboard.add(types.InlineKeyboardButton(text=name, callback_data=data))
    return keyboard


def build_card_text(detail):
    rows = [
        f"ФИО: <b>{esc(str(detail.get('FIO') or '-'))}</b>",
        f"Год рождения: {esc(str(detail.get('BIRTH_YEAR') or '-'))}",
        f"Год расстрела: {esc(str(detail.get('EXECUTION_YEAR') or '-'))}",
    ]

    # В "Номер сектора" ссылка на Схему комплекса
    if detail.get("SECTOR_NUMBER"):
        rows.append(f"Номер сектора: {esc(str(detail['SECTOR_NUMBER']))} (<a href='{map_url}'>Схема комплекса</a>)")
    else:
        rows.append("Номер сектора: -")

    # В "Номер стелы" ссылка на Схему сектора
    if detail.get("STELE_NUMBER"):
        sector_photo = detail.get("SECTOR_PHOTO") or ""
        stele_link = f" (<a href='{img_url + sector_photo}'>Схема сектора</a>)" if sector_photo else ""
        rows.append(f"Номер стелы: {esc(str(detail['STELE_NUMBER']))}{stele_link}")
    else:
        rows.append("Номер стелы: -")

    if detail.get("SLAB_PHOTO") and detail.get("SLAB_NUMBER"):
        rows.append(f"Номер плиты: {esc(str(detail['SLAB_NUMBER']))} (фото приложено)")
    elif detail.get("SLAB_PHOTO"):
        rows.append("Номер плиты: - (фото приложено)")
    elif detail.get("SLAB_NUMBER"):
        rows.append(f"Номер плиты: {esc(str(detail['SLAB_NUMBER']))}")
    else:
        rows.append("Номер плиты: -")

    if detail.get("STELE_COORD"):
        gmap_link = detail.get("STELE_GMAP_LINK") or "#"
        rows.append(f"Координаты стелы: <a href='{esc(str(gmap_link))}'>{esc(str(detail['STELE_COORD']))}</a>")
    else:
        rows.append("Координаты стелы: -")

    if not detail.get("STELE_NUMBER"):
        rows.append(no_stele_text)
    elif not detail.get("SLAB_PHOTO"):
        # плита установлена, но не отснята (8 плит, ~350 человек) — тот же текст, что на сайте
        where = f"на плите {esc(str(detail['SLAB_NUMBER']))}" if detail.get("SLAB_NUMBER") else "на плите"
        rows.append(
            f"\nФотографии этой плиты в архиве пока нет — мы работаем над её появлением. "
            f"Имя увековечено {where} стелы {esc(str(detail['STELE_NUMBER']))}."
        )

    return "\n".join(rows)


def download_slab_photo(detail):
    """Скачивает фото плиты с нашего сервера. Возвращает bytes или None."""
    raw_photo_url = img_url + detail["SLAB_PHOTO"]
    photo_url = urllib.parse.quote(raw_photo_url, safe=":/?=&")
    try:
        response = requests.get(photo_url, timeout=10)
        if response.status_code == 200:
            return response.content
        logger.error(f"Failed to download photo. Status: {response.status_code}, URL: {photo_url}")
    except Exception as e:
        logger.error(f"Failed to download photo {photo_url}: {e}")
    return None


def send_card(chat_id, detail):
    text = build_card_text(detail)
    photo = download_slab_photo(detail) if detail.get("SLAB_PHOTO") else None

    if photo and len(text) <= 1024:
        # карточка и фото одним сообщением (текст — подпись к фото)
        bot.send_photo(chat_id, photo, caption=text, parse_mode="html")
    elif photo:
        bot.send_message(chat_id, text, parse_mode="html", disable_web_page_preview=True)
        bot.send_photo(chat_id, photo, caption=f"Плита: {detail.get('FIO') or ''}")
    else:
        if detail.get("SLAB_PHOTO"):
            # фото должно быть, но сервер его не отдал — без ссылок и адресов наружу
            text += "\n\nФото плиты временно недоступно, попробуйте позже."
        elif detail.get("SLAB_NUMBER"):
            # плита есть, но её фото пока нет в архиве (В1-В3, Н1-Н3, Н5-Н6)
            text += "\n\nФотографии этой плиты в архиве пока нет — мы работаем над её появлением."
        bot.send_message(chat_id, text, parse_mode="html", disable_web_page_preview=True)


def send_compact_list(chat_id, details):
    total = len(details)
    lines = [f"Найдено: {total}. Список ниже — нажмите на фамилию, чтобы скопировать её, и отправьте сообщением (при полных тёзках добавьте год)."]
    for d in details[:MAX_LIST_LINES]:
        fio = esc(str(d.get("FIO") or "-"))
        birth = esc(str(d.get("BIRTH_YEAR") or "?"))
        execution = esc(str(d.get("EXECUTION_YEAR") or "?"))
        lines.append(f"• <code>{fio}</code> ({birth}–{execution})")
    if total > MAX_LIST_LINES:
        lines.append(f"…и ещё {total - MAX_LIST_LINES}. Уточните запрос: добавьте инициалы или год (например, «Иванов И. 1937»).")
    bot.send_message(chat_id, "\n".join(lines), parse_mode="html")


def handle_search(chat_id, raw_text):
    try:
        name, year = parse_query(raw_text or "")

        if not name:
            bot.send_message(chat_id, enter_surname_text)
            return

        details = find_by_name(name, year)

        if details is None:
            bot.send_message(chat_id, "Произошла техническая ошибка. Мы уже работаем над ней. Пожалуйста, попробуйте позже.")
            return

        if not details:
            suggestions = get_fuzzy_suggestions(name)
            if suggestions:
                bot.send_message(
                    chat_id,
                    f"Извините, мы никого не нашли с фамилией <b>{esc(name)}</b>.\n\nВозможно, вы имели в виду (нажмите на вариант):",
                    parse_mode="html",
                    reply_markup=suggestion_keyboard(suggestions),
                )
            else:
                bot.send_message(chat_id, "Извините, мы никого не нашли с такой фамилией. Проверьте правильность написания.")
            return

        if len(details) > MAX_CARDS:
            send_compact_list(chat_id, details)
            return

        for detail in details:
            send_card(chat_id, detail)

    except Exception as e:
        logger.error(f"Unexpected error in handle_search for query '{raw_text}': {e}")
        bot.send_message(chat_id, "Произошла техническая ошибка. Мы уже работаем над ней. Пожалуйста, попробуйте позже.")


@bot.message_handler(commands=["start"])
def start(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btn_map = types.KeyboardButton(text="🗺 Схема комплекса")
    btn_help = types.KeyboardButton(text="ℹ️ Как использовать бот?")
    keyboard.add(btn_map, btn_help)
    bot.send_message(message.chat.id, start_text, parse_mode="html", reply_markup=keyboard)


@bot.message_handler(commands=["help"])
def help_command(message: types.Message):
    bot.send_message(message.chat.id, help_text, parse_mode="html")


@bot.message_handler(func=lambda message: message.text in ["Схема комплекса", "🗺 Схема комплекса"])
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
        bot.send_message(message.chat.id, "Не удалось загрузить схему комплекса. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text in ["Как использовать бот?", "ℹ️ Как использовать бот?"])
def send_help(message: types.Message):
    bot.send_message(message.chat.id, text=help_text, parse_mode="html")


@bot.message_handler(func=lambda message: True)
def send_person_details(message: types.Message):
    handle_search(message.chat.id, message.text)


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("s:"))
def on_suggestion_click(call):
    # нажатие на кнопку-подсказку: ищем предложенную фамилию
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    handle_search(call.message.chat.id, call.data[2:])


@bot.callback_query_handler(func=lambda call: True)
def unknown_callback(call) -> None:
    logger.info("Unknown callback %s", call.data)


@bot.message_handler(content_types=content_type_media)
def reply_not_text(message) -> None:
    bot.send_message(message.chat.id, not_text_reply)


@bot.message_handler(content_types=content_type_service)
def log_service(message) -> None:
    logger.info("Service message %s", message.content_type)


def setup_bot_commands():
    """Меню команд (кнопка «/» в Telegram)."""
    try:
        bot.set_my_commands([
            types.BotCommand("start", "Главное меню"),
            types.BotCommand("help", "Как пользоваться ботом"),
        ])
    except Exception as e:
        logger.error(f"Failed to set bot commands: {e}")


def run_local():
    bot.delete_webhook()
    bot.infinity_polling(none_stop=True, timeout=60)


@app.route("/" + TOKEN, methods=["POST"])
def getMessage():
    bot.process_new_updates([types.Update.de_json(request.stream.read().decode("utf-8"))])
    return "!", 200


@app.route("/")
def webhook():
    if not url:
        return "WEBHOOK_URL is not set", 500
    bot.remove_webhook()
    bot.set_webhook(url=url + TOKEN)
    return "!", 200


if __name__ == "__main__":
    setup_bot_commands()
    if POLLING_MODE:
        run_local()
    else:
        app.run(threaded=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
