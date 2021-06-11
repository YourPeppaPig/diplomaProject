import pandas as pd
import glob
import os
from settings import Settings
import db_helper
#import db_helperr

# Переменные, которые теоретически могут изменяться
# Количество регионов
subjects_count = Settings.subjects_count
# Путь к папке группы, к которой принадлежит файл
path = Settings.rosstat_path

# Переменные, уникальные для файла
# Имя файла
filename = '05-02 оборот розничной торговли пищевыми продуктами.xls';
# Папка, в которой лежит файл
list_of_files = glob.glob('{0}/*'.format(path))
folder = max(list_of_files, key=os.path.getctime)
# Коды листов файла
codes = ['оборот_розничной_торговли_пищевыми_продуктами_млн',
         'оборот_розничной_торговли_пищевыми_продуктами_%_соотв_месяц',
         'оборот_розничной_торговли_пищевыми_продуктами_млн_соотв_период',
         'оборот_розничной_торговли_пищевыми_продуктами_%_пред_месяц']

# Статичные переменные
full_file_name = folder + "/05 торговля" + '/' + filename

# functions_start
def parsing_sheet(file, xfile_id, code):
    global filename, db_helper
    # Получаем indicator_id
    xindicator_id = db_helper.select_id_from_indicator(code, filename)
    # Получение данных
    for level in file.columns:
        xperiod_id, xperiod_value = db_helper.select_period_id_value_from_mapping(filename, level[2], level[3])
        for indx in range(len(file.index)):
            xvalue = file.iloc[indx][level]
            if pd.isna(xvalue) or xvalue == '-' or xvalue == '…':
                # ДОБАВИТЬ
                # period_val = db_helper.select_period_by_id(xperiod_id)
                # region_val = db_helper.select_region_by_id(xregion_id)
                # indicator_val = db_helper.select_indicator_by_id(xindicator_id)
                # error = db_helper.messageForLoggingDQ(period_val, region_val, indicator_val, значение: '{0}').format(xvalue)
                # db_helper.insert_data_logging(error, 'medium')
                # ДОБАВИТЬ
                continue
            xregion_id = db_helper.select_region_id_from_mapping(filename, file.index[indx], xperiod_value)
            # Поставить регион раньше проверки value
            if xregion_id is None:
                continue
            db_helper.insert_region_period_indicators(xregion_id, xindicator_id, xperiod_id, xvalue, xfile_id)


# functions_end

try:
    db_helper = db_helper.DBHelper()
    # db_helper = db_helperr.DBHelper()
    last_file_id = db_helper.select_file_id_from_incoming_files(filename)
    xfile_id = db_helper.insert_incoming_files(filename)
    db_helper.commit()

    for i in range(4):
        code = codes[i]
        file = pd.read_excel(full_file_name, header=[0, 1, 2, 3], sheet_name=i, index_col=0);
        file.drop(file.tail(len(file.index) - subjects_count).index, inplace=True)
        parsing_sheet(file, xfile_id, code)

    db_helper.update_status_incoming_files(xfile_id)
    db_helper.delete_region_period_indicators_by_file_id(last_file_id)

except Exception as error:
    # ДОБАВИТЬ
    # db_helper.insert_data_logging(error, 'high')
    # ДОБАВИТЬ
    db_helper.commit()
    print("Ошибка транзакции. Остановка парсинга. Отмена всех операций транзакции. Сообщение: ", error)
    if xfile_id is not None:
        db_helper.delete_region_period_indicators_by_file_id(xfile_id)

finally:
    db_helper.close_connection()