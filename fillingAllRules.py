import db_helperr

#Используемые индикаторы для расчета разл средних
#т.к. в базе присутствуют данные по моделям LSTM, SARIMA, для которых правила не нужны
#на данный момент берем
indicators = 27
regions = 97
periods = 156

try:
    db_helper = db_helperr.DBHelper()
    print('Выберите id правила:')
    rule_id = input()
    i = indicators
    while i > 0:
        r = regions
        while r > 0:
            p = periods
            while p > 0:
                xvalue = db_helper.select_value_from_regperindc(p,r,i)
                if xvalue is not None:
                    db_helper.insert_data_rules_checking(rule_id, r, i, p)
                p = p - 1
            r = r - 1
        i = i - 1

except Exception as error:
    db_helper.insert_data_logging(error, 'low')
    print("Ошибка транзакции. Отмена всех операций. Сообщение: ", error)

finally:
    db_helper.close_connection()
