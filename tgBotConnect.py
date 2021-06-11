import requests

name = "DataLoadingBot"
token = "1358150978:AAGw9i32lKRH4M6mpV59LrlqKylPrntJnQA"
chat_id = "502769202"
api_link = "https://api.telegram.org/bot"

def sendMessage(message):
    send_message = requests.get(api_link + token +
                                f"/sendMessage?chat_id={chat_id}&text={message}")
