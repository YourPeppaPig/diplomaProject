import db_helperr
import tgBotConnect
from datetime import datetime, date, time

db_helper = db_helperr.DBHelper()
logs_tuple = db_helper.select_logs_from_logging(datetime.date(datetime.today()))
db_helper.commit()

errorMessage = ' '
for x in logs_tuple:
    errorMessage = errorMessage + ' ' + x[1] + '. Приоритет: ' + x[2] + '\n\n'
tgBotConnect.sendMessage("~~~~Журналирование ошибок~~~~")
tgBotConnect.sendMessage(errorMessage)
