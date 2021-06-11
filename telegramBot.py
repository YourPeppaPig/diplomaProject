import telebot
#бот
import tgBotConnect
import tgNotification

message = "Hello!"
tgNotification.sendMessage(message)

bot = telebot.TeleBot(tgBotConnect.Configure.token)

@bot.message_handler(commands=['start', 'help'])
def sendWelcome(message):
    bot.send_message(message.chat.id, 'Доброго времени суток! Я создан для мониторинга загрузки данных на сервер.')

@bot.message_handler(func= lambda m: True)
def sendFile(message):
    bot.reply_to(message, 'Доброго времени суток! Отправь мне файл xls для дальнейшей обработки.')

@bot.message_handler(content_types=['document'])
def handleFileXls(message):
    try:
        file_info = bot.get_file(message.document.file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        src = r'C:\Users\79033\Desktop\фото\\' + message.document.file_name;
        with open(src, 'wb') as new_file:
            new_file.write(downloaded_file)
        bot.reply_to(message, "Мне выгрузить данные из файла?")
    except Exception as e:
        bot.reply_to(message, e)
    bot.stop_polling()

bot.polling()
