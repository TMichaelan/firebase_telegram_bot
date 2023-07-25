import asyncio
import datetime
import sqlite3
import schedule
import time
from datetime import datetime
from google.cloud import firestore
from telegram import Bot
from telegram.error import TelegramError

bot_token = ''
firebase_credentials = ''
chat_id = ''

bot = Bot(token=bot_token)
db = firestore.Client.from_service_account_json(firebase_credentials)
collections = ['announcements', 'notifications', 'notificationsagr']

# Connect to SQLite database
conn = sqlite3.connect('processed_docs.db')
c = conn.cursor()

# Create table if not exists
c.execute('''CREATE TABLE IF NOT EXISTS processed_docs
             (doc_id text)''')
conn.commit()

async def send_message(message):
    while True:
        try:
            await bot.send_message(chat_id=chat_id, text=f"{message}", parse_mode='HTML', disable_web_page_preview='True')
            break
        except TelegramError as e:
            print(e)
            print("An error occurred while sending the message. Retrying in 5 seconds.")
            await asyncio.sleep(5)

async def send_photo_message(message,photo):

    counter = 0

    while True:
        try:
            await bot.send_photo(chat_id=chat_id,photo=photo, caption=f"{message}", parse_mode='HTML')
            break
        except TelegramError as e:
            print(e)
            counter += 1
            if counter == 2:
                break
            print("An error occurred while sending the message. Retrying in 5 seconds.")

            
def check_for_updates():
    for col in collections:
        if col != 'notificationsAggr':
            docs = db.collection(col).stream()
            for doc in docs:
                c.execute("SELECT * FROM processed_docs WHERE doc_id = ?", (doc.id,))
                if c.fetchone() is None:

                    if col == "notifications":
                        doc_dict = doc.to_dict()
                        message = f"<b>{doc_dict['title']}</b>\n{doc_dict['body']}"
                        asyncio.run(send_message(message))
                    elif col == "announcements":
                        doc_dict = doc.to_dict()
                        message =  f"<b>{doc_dict['title']}</b>\n{doc_dict['body']}\n<b><a href=\"{doc_dict['link']}\">Follow Link!</a></b>"
                        photo = doc_dict['image']
                        if photo == "":
                            print("no photo")
                            asyncio.run(send_message(message))
                        else:
                            asyncio.run(send_photo_message(message,photo))

                    c.execute("INSERT INTO processed_docs VALUES (?)", (doc.id,))
                    conn.commit()
        else:
            docs = db.collection('notificationsAggr').stream()

            for doc in docs:
                notifications = doc.to_dict()
                for notification in notifications["data"]:
                    id = notification.get('id')
                    c.execute("SELECT * FROM processed_docs WHERE doc_id = ?", (id,))
                    if c.fetchone() is None:
                        title = notification.get('title')
                        body = notification.get('body')
                                               
                        message =  f"<b>{title}</b>\n{body}"

                        asyncio.run(send_message(message))

                        c.execute("INSERT INTO processed_docs VALUES (?)", (id,))
                        conn.commit()
                break  

    print('Done checking ' + str(datetime.now()))

schedule.every(5).seconds.do(check_for_updates)

while True:
    schedule.run_pending()
    time.sleep(1)

