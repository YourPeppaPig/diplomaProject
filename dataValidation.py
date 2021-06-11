import db_helperr

try:
    db_helper = db_helperr.DBHelper()
    db_helper.commit()
    infoRules = db_helper.select_ids_from_rules_checking()
    count = 0
    for infoRule in infoRules:
        rule = db_helper.select_code_rule(infoRule[1])
        rule = rule.lower()
        value = db_helper.select_value_from_regperindc( infoRule[2] ,infoRule[3], infoRule[4])
        value = value[0]
        print(infoRule, rule, value)

        if eval(rule):
            command = db_helper.update_statusT_rules_check(infoRule[0])
            print("Да")
        else:
            command = db_helper.update_statusF_rules_check(infoRule[0])
            print("Нет")
            period_val = db_helper.select_period_by_id(infoRule[2])
            region_val = db_helper.select_region_by_id(infoRule[3])
            indicator_val = db_helper.select_indicator_by_id(infoRule[4])
            rule_val = db_helper.select_rule_dscription_by_id(infoRule[1])

            message = db_helper.messageForLoggingDQ(period_val, region_val, indicator_val, rule_val)
            db_helper.insert_data_logging(message, 'medium')
        count = count + 1
    print(count)

except Exception as error:
    db_helper.insert_data_logging(error, 'high')
    db_helper.commit()
    print("Ошибка транзакции. Отмена всех операций. Сообщение: ", error)

finally:
    db_helper.close_connection()
