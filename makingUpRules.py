import db_helperr

#Ввод данных (пока через консоль)
def enterDataRule():
    print('Введите id user')
    xuser_id = int(input())
    print('Введите код python')
    xcode = input()
    print ('Лог ошибки')
    xdescription_log = input()
    return [xuser_id, xcode, xdescription_log]

try:
    db_helper = db_helperr.DBHelper()
    entryDataRule = enterDataRule()
    if entryDataRule[0] is None or entryDataRule[0] == ''\
            or entryDataRule[1] is None or entryDataRule[1] == ''\
            or entryDataRule[2] is None or entryDataRule[2] == '':
        errorMessage = "Была произведена попытка вставки значения NULL в таблицу Rules"
        db_helper.exception_log(errorMessage)
    else:
        db_helper.insert_data_rules(entryDataRule[0], entryDataRule[1], entryDataRule[2])

except Exception as error:
    db_helper.insert_data_logging(error, 'low')
    print("Ошибка транзакции. Сообщение: ", error)

finally:
    db_helper.close_connection()
