import db_helperr

#Ввод данных (пока через консоль)
def enterDataRulesCheck():
    print('Выберите правило:')
    rule = input()
    xrule = db_helper.select_id_from_rules(rule)

    print('Выберите регион:')
    region = input()
    xregion = db_helper.select_id_from_region(region)

    print('Выберите индикатор:')
    indicator_code = input()
    indicator_xls_filename = input()
    xindicator = db_helper.select_id_from_indicator(indicator_code, indicator_xls_filename)

    print('Выберите период:')
    period = input()
    xperiod = db_helper.select_id_from_period(period)

    return [xrule, xregion, xindicator, xperiod]

try:
    db_helper = db_helperr.DBHelper()
    entryDataRulesCheck = enterDataRulesCheck()
    db_helper.commit()
    print(entryDataRulesCheck)

    db_helper.insert_data_rules_checking(
        entryDataRulesCheck[0], entryDataRulesCheck[1],
        entryDataRulesCheck[2], entryDataRulesCheck[3]
    )

except Exception as error:
    db_helper.insert_data_logging(error, 'low')
    print("Ошибка транзакции. Отмена всех операций. Сообщение: ", error)

finally:
    db_helper.close_connection()
