import logging
import mysql.connector
from mysql.connector import Error
from telebot import TeleBot, types, logger

import os
from flask import Flask, request

start_text = """
<b>Приветствуем! Это бот-мартиролог мемориала жертв политических репрессий 12 километр в Екатеринбурге.</b>

<u><b>Что умеет этот бот?</b></u>

Бот содержит полный список захороненных в Мемориальном комплексе 12 километр жертв Большого Террора. С помощью поискового запроса вы можете получить карточку репрессированного. 
Также вы можете изучить схему мемориального комплекса и найти нужную стелу. Для этого нажмите кнопку "Схема мемориала"

<u><b>Как использовать бот?</b></u>

Бот можно использовать двумя способами:
1. Обычный поиск. Введите фамилию репрессированного и получите карточки всех репрессированных с такой фамилией.
2. Поиск с подсказками. Нажмите кнопку "Поиск с подсказками", после этого начните вводить данные репрессированного. В режиме онлайн перед вами начнут появляться фамилии репрессированных, вы можете выбрать интересующего вас человека.

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

app = Flask(__name__)

IS_LOCAL = os.environ.get("IS_LOCAL") or True

if IS_LOCAL is True:
    env = ".env_local"
    with open(env) as f:
        for l in f.readlines():
            if '=' in l:
                os.environ[l.split('=')[0].strip()] = l.split('=')[1].strip()


TOKEN = os.environ.get("TOKEN")
bot = TeleBot(TOKEN)
logger.setLevel(logging.DEBUG)
url = "https://martirolog-89a3aa406540.herokuapp.com/"
img_url = "http://files.ekmemorial.org/martirolog/"
map_url = img_url + "karty/sector-all.png"


conn = {
    "host": os.environ.get("DATABASE_HOST") or "127.0.0.1",
    "port": int(os.environ.get("DATABASE_PORT") or 3306),
    "user": os.environ.get("DATABASE_USER") or "root",
    "password": os.environ.get("DATABASE_PASSWORD"),
}
db_name = os.environ.get("DATABASE_NAME")
table_name = os.environ.get("TABLE_NAME")

def find_by_name(name):
    results = []
    try:
        connection = mysql.connector.connect(**conn)

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM {db_name}.{table_name} WHERE LOWER(FIO) LIKE '{name.lower()}%'")
            results = cursor.fetchall()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
    return results

@bot.message_handler(commands=['start'])
def start(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
    keyboard.add(types.InlineKeyboardButton(text='Поиск с подсказками'))
    keyboard.add(types.InlineKeyboardButton(text='Схема комплекса'))
    bot.send_message(message.chat.id, start_text, parse_mode='html', reply_markup=keyboard)

def find_fio_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    switch_button = types.InlineKeyboardButton(text="Начать", switch_inline_query_current_chat="")
    keyboard.add(switch_button)
    return keyboard

@bot.message_handler(func=lambda message: message.text == 'Поиск с подсказками')
def find_people(message: types.Message):
    bot.send_message(message.chat.id, "Нажмите чтобы начать поиск", reply_markup=find_fio_keyboard())

@bot.message_handler(func=lambda message: message.text == 'Схема комплекса')
def send_schema(message: types.Message):
    bot.send_message(message.chat.id, text = f"<a href='{map_url}'>Схема комплекса</a>", parse_mode='html')

@bot.inline_handler(func=lambda query: len(query.query) > 0)
def find_by_fio(query):
    try:
        people = find_by_name(text)[:5]
        lines = [f'{row["FIO"]}\n' for row in people]
        results = []
        for index, line in enumerate(lines):
            results.append(
                types.InlineQueryResultArticle(
                    id=str(index),
                    title=line,
                    input_message_content=types.InputTextMessageContent(message_text=line),
                )
            )
        bot.answer_inline_query(query.id, results, cache_time=1)
    except Exception as e:
        print(e)

@bot.message_handler(func=lambda message: True)
def send_person_details(message):
    try:
        fio = message.text
        if 'ФИО: ' in fio:
            fio = fio.split('ФИО: ')[-1]
        details = find_by_name(fio)

        if not details:
            response = "Извините, мы никого не нашли с такой фамилмей"
        elif len(details) > 6:
            response = "По вашему запросу найдено больше шести человек, пожалуйста уточните запрос"
        else:
            response = ""
            for detail in details:
                response_rows = [
                    f"ФИО: <b>{detail['FIO']}</b>",
                    f"Год рождения : {detail['BIRTH_YEAR']}",
                    f"Год расстрела: {detail['EXECUTION_YEAR']}",
                ]
                if detail['SECTOR_NUMBER'] != "":
                    response_rows.append(f"Номер сектора: {detail['SECTOR_NUMBER']} (<a href='{img_url + detail['SECTOR_PHOTO']}'>Схема сектора</a>)")
                else:
                    response_rows.append("Номер сектора: -")
                if detail['STELE_NUMBER'] != "":
                    response_rows.append(f"Номер стелы: {detail['STELE_NUMBER']}")
                else:
                    response_rows.append("Номер стелы: -")
                if detail['SLAB_PHOTO'] != "" and detail['SLAB_NUMBER'] != "":
                    response_rows.append(f"Номер плиты: {detail['SLAB_NUMBER']} (<a href='{img_url + detail['SLAB_PHOTO']}'>Фото плиты</a>)")
                elif detail['SLAB_PHOTO'] != "":
                    response_rows.append(f"Номер плиты: - (<a href='{img_url + detail['SLAB_PHOTO']}'>Фото плиты</a>)")
                else:
                    response_rows.append("Номер плиты: -")
                if detail['STELE_COORD'] != "":
                    response_rows.append(f"Координаты стелы: <a href='{detail['STELE_GMAP_LINK']}'>{detail['STELE_COORD']}</a>")
                else:
                    response_rows.append("Координаты стелы: -")
                if detail['STELE_NUMBER'] == "":
                    response_rows.append(no_stele_text)
                response += "\n".join(response_rows)
                response += "\n\n"

    except ValueError:
        response = "Please send a valid Name."

    except Exception as e:
        response = f"An error occurred: {e}"

    bot.send_message(message.chat.id, response, parse_mode='html', disable_web_page_preview=True)


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
    if IS_LOCAL:
        run_local()
    else:
        app.run(threaded=True, host="0.0.0.0", port=int(os.environ.get('PORT', 5000)))
