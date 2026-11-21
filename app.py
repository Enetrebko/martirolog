"""Телеграм-бот мартиролога мемориала «12 километр» (@ek_12km_bot).

Как устроен:
- таблица мартиролога читается из базы в память и перечитывается раз в несколько минут;
  поиск идёт по памяти и не зависит от точек, пробелов и регистра в инициалах;
- фото плит берутся с диска сервера (PHOTO_DIR); Telegram-идентификаторы уже отправленных
  фото запоминаются, повторная отправка идёт без загрузки файла;
- все адреса и пути задаются переменными окружения (/etc/martirolog/env), в коде только
  значения по умолчанию.

Переменные окружения:
  TOKEN                      токен бота (обязательно)
  DATABASE_HOST / _PORT / _USER / _PASSWORD / DATABASE_NAME / TABLE_NAME
  PHOTO_DIR                  папка с фото плит на сервере (по умолчанию /var/www/files/martirolog_new)
  PUBLIC_PHOTO_URL           публичный адрес той же папки (по умолчанию https://12km.org/photos/)
  IS_DEV                     true — режим разработки: читать .env_local, подробный журнал
"""
import html
import json
import logging
import os
import re
import threading
import time
import urllib.parse
from collections import Counter

import mysql.connector
import requests
from telebot import TeleBot, types, logger
from telebot.util import content_type_media, content_type_service

# Нечёткий поиск (подсказки при опечатках)
try:
    from rapidfuzz import fuzz, process as fuzz_process
except ImportError:
    fuzz = None
    fuzz_process = None
    print("Внимание: библиотека rapidfuzz не установлена. Подсказки при опечатках работать не будут.")

# ---------------------------------------------------------------- тексты

start_text = """
<b>Приветствуем! Это бот-мартиролог мемориала жертв политических репрессий 12 километр в Екатеринбурге.</b>

<u><b>Что умеет этот бот?</b></u>

В боте — имена с плит Мемориального комплекса 12 километр: расстрелянных по политическим обвинениям в 1930-е – 1940-е годы и захороненных на 12-м километре Московского тракта. С помощью поискового запроса вы можете получить карточку репрессированного.
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
К фамилии можно добавить инициалы или имя и отчество — в любом виде: "Иванов И.И.", "Иванов И И", "Иванов Иван Иванович".

🔍 <b>Уточнение по году:</b>
Если найдено слишком много людей, добавьте к фамилии год рождения или год расстрела (например, "Иванов 1890" или "Петров 1937"). Поиск всегда начинается с фамилии — искать только по году нельзя.

Если вы не уверены в написании фамилии, просто введите её, и бот предложит похожие варианты.
"""

enter_surname_text = "Пожалуйста, введите фамилию. Поиск по одному только году не поддерживается — год можно добавить к фамилии для уточнения (например, «Иванов 1937»)."
not_text_reply = "Бот понимает только текстовые сообщения. Введите фамилию репрессированного — например: Иванов"
tech_error_text = "Произошла техническая ошибка. Мы уже работаем над ней. Пожалуйста, попробуйте позже."
not_found_text = "Извините, мы никого не нашли с такой фамилией. Проверьте правильность написания."

# ---------------------------------------------------------------- настройки


def env_bool(name: str, default: bool = False) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


IS_DEV = env_bool("IS_DEV", default=False)

# Для запуска на своём компьютере: настройки из файла .env_local рядом с кодом
if os.path.exists(".env_local") and env_bool("IS_DEV", default=True):
    with open(".env_local", encoding="utf-8") as f:
        for line in f:
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
    IS_DEV = env_bool("IS_DEV", default=True)

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise SystemExit("Ошибка: переменная окружения TOKEN не задана (токен бота).")

DB_CONFIG = {
    "host": os.environ.get("DATABASE_HOST") or "127.0.0.1",
    "port": int(os.environ.get("DATABASE_PORT") or 3306),
    "user": os.environ.get("DATABASE_USER") or "root",
    "password": os.environ.get("DATABASE_PASSWORD"),
    "connection_timeout": 5,  # не ждать зависшую базу дольше 5 секунд
}
DB_NAME = os.environ.get("DATABASE_NAME") or ""
TABLE_NAME = os.environ.get("TABLE_NAME") or ""
for _label, _value in (("DATABASE_NAME", DB_NAME), ("TABLE_NAME", TABLE_NAME)):
    if not re.fullmatch(r"\w+", _value):
        raise SystemExit(f"Ошибка: переменная окружения {_label} не задана или содержит недопустимые символы.")

PHOTO_DIR = os.path.normpath(os.environ.get("PHOTO_DIR") or "/var/www/files/martirolog_new")
PUBLIC_PHOTO_URL = (os.environ.get("PUBLIC_PHOTO_URL") or "https://12km.org/photos/").rstrip("/") + "/"
PHOTO_ON_DISK = os.path.isdir(PHOTO_DIR)  # на сервере — да; на своём компьютере фото берутся по сети
MAP_PHOTO = "karty/sector-all.png"

# Куда запоминать идентификаторы фото. STATE_DIRECTORY задаёт systemd (StateDirectory= в службе).
# В имени файла — номер бота (часть токена до двоеточия, не секрет): у тестового бота свой файл.
STATE_DIR = (os.environ.get("STATE_DIRECTORY") or ".").split(":")[0]
FILE_ID_PATH = os.path.join(STATE_DIR, f"file_ids_{TOKEN.split(':')[0]}.json")

MAX_QUERY_LEN = 100     # максимальная длина запроса пользователя
MAX_CARDS = 10          # больше этого — показываем компактный список
MAX_LIST_LINES = 90     # в одном сообщении Telegram не больше 100 выделенных фрагментов
MAX_LIST_CHARS = 3800   # и не больше 4096 знаков
FUZZY_SUGGESTIONS = 3   # сколько подсказок-кнопок предлагать
FUZZY_CUTOFF = 72       # порог похожести фамилий, 0–100
DATA_TTL = 300          # раз в 5 минут перечитываем таблицу из базы
DATA_RETRY = 30         # если база не ответила — следующая попытка через 30 секунд

bot = TeleBot(TOKEN)
logger.setLevel(logging.DEBUG if IS_DEV else logging.INFO)
esc = html.escape

# ---------------------------------------------------------------- разбор имён

_WORD_RE = re.compile(r"[a-zа-я]+(?:-[a-zа-я]+)*")
_YEAR_RE = re.compile(r"\b\d{4}\b")


def normalize(text):
    """Нижний регистр, ё→е, все виды тире → дефис."""
    text = str(text or "").lower().replace("ё", "е")
    return re.sub(r"[‐‑‒–—−]", "-", text)


def split_name(text):
    """«БИТНЫЙ К..А.» → («битный», «ка»); «Го-Хун-Бин» → («го-хун-бин», «»).

    Фамилия — первое слово; всё остальное (инициалы или имя) склеивается в одни буквы,
    поэтому точки, запятые, пробелы и регистр на поиск не влияют.
    """
    words = _WORD_RE.findall(normalize(text))
    if not words:
        return "", []
    return words[0], [w.replace("-", "") for w in words[1:]]


def parse_query(text):
    """Запрос пользователя → (фамилия, слова после фамилии, год или None)."""
    text = (text or "").strip()[:MAX_QUERY_LEN]
    # наследие старого формата: если переслали строку карточки "ФИО: ..."
    if "ФИО: " in text:
        text = text.split("ФИО: ")[-1]
    years = _YEAR_RE.findall(text)
    surname, rest = split_name(_YEAR_RE.sub(" ", text))
    return surname, rest, (years[0] if years else None)


# ---------------------------------------------------------------- данные в памяти

_data = {"entries": [], "surnames": Counter(), "ts": 0.0}
_data_lock = threading.Lock()


def _fetch_rows():
    connection = mysql.connector.connect(**DB_CONFIG)
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM `{DB_NAME}`.`{TABLE_NAME}`")
        rows = cursor.fetchall()
        cursor.close()
        return rows
    finally:
        connection.close()


def build_entries(rows):
    """Готовит строки таблицы к поиску: фамилия, буквы инициалов, всё слитно."""
    entries = []
    for row in rows:
        surname, rest = split_name(row.get("FIO"))
        rest = "".join(rest)
        entries.append({
            "surname": surname,
            "rest": rest,
            "compact": surname.replace("-", "") + rest,
            "years": {str(row.get("BIRTH_YEAR") or "").strip(), str(row.get("EXECUTION_YEAR") or "").strip()},
            "sort": (normalize(row.get("FIO")), str(row.get("BIRTH_YEAR") or "")),
            "row": row,
        })
    entries.sort(key=lambda e: e["sort"])
    return entries


def get_entries():
    """Возвращает таблицу из памяти; раз в DATA_TTL секунд перечитывает её из базы.

    Если база не отвечает, бот продолжает работать на последних прочитанных данных.
    """
    now = time.time()
    if now - _data["ts"] < DATA_TTL:
        return _data["entries"]
    # перечитывает кто-то один; остальные в это время пользуются прежними данными
    if not _data_lock.acquire(blocking=not _data["entries"]):
        return _data["entries"]
    try:
        if time.time() - _data["ts"] < DATA_TTL:
            return _data["entries"]
        try:
            entries = build_entries(_fetch_rows())
            if not entries:
                raise RuntimeError("таблица пуста")
            _data["entries"] = entries
            _data["surnames"] = Counter(e["surname"] for e in entries if e["surname"])
            _data["ts"] = time.time()
            logger.info("Таблица мартиролога прочитана: %d записей", len(entries))
        except Exception as e:
            logger.error("Не удалось прочитать таблицу из базы: %s", e)
            _data["ts"] = time.time() - DATA_TTL + DATA_RETRY
    finally:
        _data_lock.release()
    return _data["entries"]


# ---------------------------------------------------------------- поиск


def match_entries(entries, surname, rest):
    """Ищет людей. С инициалами — фамилия точно; без них — по началу фамилии."""
    if rest:
        letters = "".join(rest)                 # «и», «и» → «ии»; «ге», «бон» → «гебон»
        initials = "".join(w[0] for w in rest)  # «иван», «петрович» → «ип»
        found = [e for e in entries if e["surname"] == surname and e["rest"].startswith(letters)]
        if not found and initials != letters:
            found = [e for e in entries if e["surname"] == surname and e["rest"].startswith(initials)]
        if not found and not any(e["surname"] == surname for e in entries):
            # такой фамилии нет вовсе — возможно, имя из нескольких слов: «го хун бин», «хан ге бон»
            compact = surname.replace("-", "") + letters
            found = [e for e in entries if e["compact"].startswith(compact)]
        return found
    found = [e for e in entries if e["surname"].startswith(surname)]
    if not found:
        compact = surname.replace("-", "")
        found = [e for e in entries if e["compact"].startswith(compact)]
    # сначала точное совпадение фамилии, потом «Ивановский», «Иванова» и т. п.
    found.sort(key=lambda e: e["surname"] != surname)
    return found


def get_fuzzy_suggestions(surname, limit=FUZZY_SUGGESTIONS):
    """Возвращает до `limit` похожих фамилий из базы (подсказки при опечатке)."""
    surnames = _data["surnames"]
    if not fuzz_process or not surnames:
        return []
    matches = fuzz_process.extract(surname, list(surnames), scorer=fuzz.ratio, limit=limit, score_cutoff=FUZZY_CUTOFF)
    return [m[0] for m in matches]


def suggestion_keyboard(surnames, rest):
    keyboard = types.InlineKeyboardMarkup()
    for surname in surnames:
        data = "s:" + " ".join([surname] + rest)
        # ограничение Telegram: callback_data не длиннее 64 байт
        if len(data.encode("utf-8")) > 64:
            data = ("s:" + surname).encode("utf-8")[:64].decode("utf-8", errors="ignore")
        label = f"{surname.upper()} ({_data['surnames'][surname]})"
        keyboard.add(types.InlineKeyboardButton(text=label, callback_data=data))
    return keyboard


# ---------------------------------------------------------------- фото

_file_ids = {}
_file_ids_lock = threading.Lock()


def load_file_ids():
    try:
        with open(FILE_ID_PATH, encoding="utf-8") as f:
            saved = json.load(f)
        if isinstance(saved, dict):
            _file_ids.update(saved)
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.error("Не удалось прочитать %s: %s", FILE_ID_PATH, e)


def remember_file_id(rel, sig, file_id):
    with _file_ids_lock:
        _file_ids[rel] = {"sig": sig, "id": file_id}
        try:
            tmp = FILE_ID_PATH + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(_file_ids, f, ensure_ascii=False)
            os.replace(tmp, FILE_ID_PATH)
        except Exception as e:
            # не страшно: идентификаторы останутся в памяти до перезапуска
            logger.error("Не удалось сохранить %s: %s", FILE_ID_PATH, e)


def photo_file(rel):
    """Путь к файлу фото на диске; None, если путь выходит за пределы PHOTO_DIR."""
    full = os.path.normpath(os.path.join(PHOTO_DIR, rel))
    return full if full.startswith(PHOTO_DIR + os.sep) else None


def photo_signature(rel):
    """Отпечаток файла (время изменения и размер): заменили фото — запомненный идентификатор не годится."""
    if not PHOTO_ON_DISK:
        return "http"
    path = photo_file(rel)
    try:
        st = os.stat(path) if path else None
    except OSError:
        st = None
    return f"{int(st.st_mtime)}:{st.st_size}" if st else None


def read_photo(rel):
    try:
        if PHOTO_ON_DISK:
            with open(photo_file(rel), "rb") as f:
                return f.read()
        response = requests.get(PUBLIC_PHOTO_URL + urllib.parse.quote(rel), timeout=10)
        if response.status_code == 200:
            return response.content
        logger.error("Фото не получено, код %s: %s", response.status_code, rel)
    except Exception as e:
        logger.error("Фото не прочитано (%s): %s", rel, e)
    return None


def send_photo_cached(chat_id, rel, caption, parse_mode=None):
    """Отправляет фото. Возвращает False, если файла нет. Ошибки Telegram не глушит."""
    rel = str(rel).replace("\\", "/").lstrip("/")
    sig = photo_signature(rel)
    if sig is None:
        logger.error("Нет файла фото: %s", rel)
        return False
    cached = _file_ids.get(rel)
    if cached and cached.get("sig") == sig:
        try:
            bot.send_photo(chat_id, cached["id"], caption=caption, parse_mode=parse_mode)
            return True
        except Exception as e:
            logger.error("Запомненный идентификатор фото не сработал (%s): %s", rel, e)
            _file_ids.pop(rel, None)
    data = read_photo(rel)
    if not data:
        return False
    message = bot.send_photo(chat_id, data, caption=caption, parse_mode=parse_mode)
    try:
        remember_file_id(rel, sig, message.photo[-1].file_id)
    except Exception as e:
        logger.error("Идентификатор фото не запомнен (%s): %s", rel, e)
    return True


# ---------------------------------------------------------------- карточки


def build_card_text(detail):
    rows = [
        f"ФИО: <b>{esc(str(detail.get('FIO') or '-'))}</b>",
        f"Год рождения: {esc(str(detail.get('BIRTH_YEAR') or '-'))}",
        f"Год расстрела: {esc(str(detail.get('EXECUTION_YEAR') or '-'))}",
    ]

    # В "Номер сектора" ссылка на Схему комплекса
    if detail.get("SECTOR_NUMBER"):
        map_link = esc(PUBLIC_PHOTO_URL + MAP_PHOTO)
        rows.append(f"Номер сектора: {esc(str(detail['SECTOR_NUMBER']))} (<a href='{map_link}'>Схема комплекса</a>)")
    else:
        rows.append("Номер сектора: -")

    # В "Номер стелы" ссылка на Схему сектора
    if detail.get("STELE_NUMBER"):
        sector_photo = str(detail.get("SECTOR_PHOTO") or "")
        sector_link = esc(PUBLIC_PHOTO_URL + urllib.parse.quote(sector_photo))
        stele_link = f" (<a href='{sector_link}'>Схема сектора</a>)" if sector_photo else ""
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
        # плита установлена, но не отснята — тот же текст, что на сайте
        where = f"на плите {esc(str(detail['SLAB_NUMBER']))}" if detail.get("SLAB_NUMBER") else "на плите"
        rows.append(
            f"\nФотографии этой плиты в архиве пока нет — мы работаем над её появлением. "
            f"Имя увековечено {where} стелы {esc(str(detail['STELE_NUMBER']))}."
        )

    return "\n".join(rows)


def send_card(chat_id, detail):
    text = build_card_text(detail)
    photo = detail.get("SLAB_PHOTO")

    unavailable = "Фото плиты временно недоступно, попробуйте позже."  # без ссылок и адресов наружу

    if photo and len(text) <= 1024:
        # карточка и фото одним сообщением (текст — подпись к фото)
        if send_photo_cached(chat_id, photo, text, "html"):
            return
        text += "\n\n" + unavailable
        photo = None
    bot.send_message(chat_id, text, parse_mode="html", disable_web_page_preview=True)
    if photo:
        # текст длиннее подписи к фото — фото отдельным сообщением
        if not send_photo_cached(chat_id, photo, f"Плита: {detail.get('FIO') or ''}"):
            bot.send_message(chat_id, unavailable)


def send_cards(chat_id, details):
    """Отправляет карточки по одной; сбой одной не мешает остальным."""
    failed = 0
    for detail in details:
        try:
            send_card(chat_id, detail)
        except Exception as e:
            failed += 1
            logger.error("Карточка не отправлена: %s", e)
    if failed:
        bot.send_message(chat_id, f"Не удалось отправить карточек: {failed} из {len(details)}. Пожалуйста, попробуйте позже.")


def send_compact_list(chat_id, details):
    total = len(details)
    lines = [f"Найдено: {total}. Нажмите на имя — оно скопируется; отправьте его сообщением, чтобы получить карточку (при полных тёзках добавьте год)."]
    size = len(lines[0])
    shown = 0
    for d in details[:MAX_LIST_LINES]:
        fio = esc(str(d.get("FIO") or "-"))
        birth = esc(str(d.get("BIRTH_YEAR") or "?"))
        execution = esc(str(d.get("EXECUTION_YEAR") or "?"))
        line = f"• <code>{fio}</code> ({birth}–{execution})"
        if size + len(line) + 1 > MAX_LIST_CHARS:
            break
        lines.append(line)
        size += len(line) + 1
        shown += 1
    if total > shown:
        lines.append(f"…и ещё {total - shown}. Уточните запрос: добавьте инициалы или год (например, «Иванов И. 1937»).")
    bot.send_message(chat_id, "\n".join(lines), parse_mode="html")


def handle_search(chat_id, raw_text):
    try:
        surname, rest, year = parse_query(raw_text)
        if not surname:
            bot.send_message(chat_id, enter_surname_text)
            return

        entries = get_entries()
        if not entries:
            bot.send_message(chat_id, tech_error_text)
            return

        notes = []
        found = match_entries(entries, surname, rest)

        if not found and rest:
            # фамилия есть, а таких инициалов нет — показываем всех с этой фамилией
            found = [e for e in entries if e["surname"] == surname]
            if found:
                notes.append(f"С фамилией <b>{esc(surname.upper())}</b> найдено: {len(found)}, но с такими инициалами или именем — никого. Показываем всех с этой фамилией.")

        if not found:
            suggestions = get_fuzzy_suggestions(surname)
            if suggestions:
                bot.send_message(
                    chat_id,
                    f"Извините, мы никого не нашли с фамилией <b>{esc(surname.upper())}</b>.\n\nВозможно, вы имели в виду (нажмите на вариант):",
                    parse_mode="html",
                    reply_markup=suggestion_keyboard(suggestions, rest),
                )
            else:
                bot.send_message(chat_id, not_found_text)
            return

        if year:
            with_year = [e for e in found if year in e["years"]]
            if with_year:
                found = with_year
            else:
                notes.append(f"Среди найденных ({len(found)}) нет никого с годом рождения или расстрела {esc(year)}. Показываем без учёта года.")

        if notes:
            bot.send_message(chat_id, "\n\n".join(notes), parse_mode="html")

        details = [e["row"] for e in found]
        if len(details) > MAX_CARDS:
            send_compact_list(chat_id, details)
        else:
            send_cards(chat_id, details)

    except Exception as e:
        # текст запроса в журнал не пишем: это чьи-то родные
        logger.error("Ошибка при обработке запроса: %s: %s", type(e).__name__, e)
        try:
            bot.send_message(chat_id, tech_error_text)
        except Exception:
            pass


# ---------------------------------------------------------------- обработчики Telegram


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
        if send_photo_cached(message.chat.id, MAP_PHOTO, "Схема мемориального комплекса"):
            return
    except Exception as e:
        logger.error("Схема не отправлена: %s", e)
    bot.send_message(message.chat.id, "Не удалось загрузить схему комплекса. Пожалуйста, попробуйте позже.")


@bot.message_handler(func=lambda message: message.text in ["Как использовать бот?", "ℹ️ Как использовать бот?"])
def send_help(message: types.Message):
    bot.send_message(message.chat.id, text=help_text, parse_mode="html")


@bot.message_handler(func=lambda message: bool(message.text) and message.text.startswith("/"))
def unknown_command(message: types.Message):
    # незнакомая команда — не ищем её как фамилию, а показываем справку
    bot.send_message(message.chat.id, help_text, parse_mode="html")


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
    logger.info("Unknown callback")


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
        logger.error("Failed to set bot commands: %s", e)


if __name__ == "__main__":
    load_file_ids()
    get_entries()  # прочитать таблицу сразу; если база ещё не поднялась — прочитается при первом запросе
    setup_bot_commands()
    bot.delete_webhook()
    bot.infinity_polling(none_stop=True, timeout=60)
