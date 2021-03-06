import psycopg2
import dbConnect

class DBHelper:

    def __init__(self):
        self.connection = psycopg2.connect(
            host=dbConnect.Settings.host,
            port=dbConnect.Settings.port,
            user=dbConnect.Settings.user,
            password=dbConnect.Settings.password,
            database=dbConnect.Settings.database)
        self.connection.autocommit = False
        self.cursor = self.connection.cursor()

    def connect_database(self):
        self.connection = psycopg2.connect(
            host=dbConnect.Settings.host,
            port=dbConnect.Settings.port,
            user=dbConnect.Settings.user,
            password=dbConnect.Settings.password,
            database=dbConnect.Settings.database)
        self.connection.autocommit = False
        self.cursor = self.connection.cursor()
        #return connection, cursor

    def select(self, command):
        self.cursor.execute(command)
        result = self.cursor.fetchone()
        if result is None:
            raise psycopg2.DatabaseError("Команда {0} вернула NULL".format(command))
        else:
            return result

    def execute_query(self, command):
        print('execute')
        self.cursor.execute(command)
        self.connection.commit()

    def commit(self):
        self.connection.commit()

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

    def select_period_id_value_from_mapping(self, filename, year, month):
        command = ("SELECT period_id, date_value "
                   "FROM data.mapping_xls_period "
                   "WHERE xls_filename = '{0}' "
                   "AND xls_value_year = '{1}' "
                   "AND xls_value = '{2}'").format(filename, year, month)
        result = self.select(command)
        return result[0], result[1]

    def select_region_id_from_mapping(self, filename, region, period_value):
        command = ("SELECT region_id "
                   "FROM data.mapping_xls_region "
                   "WHERE xls_filename = '{0}' "
                   "AND xls_value = '{1}' "
                   "AND '{2}' BETWEEN date_from AND date_to").format(filename, region, period_value)
        return self.select(command)[0]

    def insert_region_period_indicators(self, region_id, indicator_id, period_id, value, file_id):
        command = ("INSERT INTO data.region_period_indicators(region_id, indicator_id, period_id, value, file_id) "
                   "VALUES ({0}, {1}, {2}, {3}, {4})").format(region_id, indicator_id, period_id, value, file_id)
        self.execute_query(command)

    def select_file_id_from_incoming_files(self, filename):
        file_command = ("SELECT id "
                        "FROM data.incoming_files "
                        "WHERE filename = '{0}' "
                        "AND status = true "
                        "ORDER BY uploaded_date DESC").format(filename)
        result = self.select(file_command)
        if len(result) == 0:
            return -1
        return result[0]

    def insert_incoming_files(self, filename):
        command = ("INSERT INTO data.incoming_files (filename, uploaded_date , status) "
                   "VALUES ('{0}', current_timestamp, false) RETURNING ID").format(filename)
        return self.select(command)[0]

    def update_status_incoming_files(self, xfile_id):
        command = "UPDATE data.incoming_files SET status = true WHERE ID = {0}".format(xfile_id)
        self.execute_query(command)

    def delete_region_period_indicators_by_file_id(self, file_id):
        command = "DELETE FROM data.region_period_indicators WHERE file_id = {0}".format(file_id)
        self.execute_query(command)

    def select_file_data_from_region_period_indicators(self, filename1, filename2):
        command = (
            'select p.value, r.name, rpi.value, rpi.indicator_id, rpi.region_id '
            'from data.region_period_indicators rpi '
            'join data.periods p on rpi.period_id = p.id '
            'join data.regions r on rpi.region_id = r.id '
            'join data.incoming_files if on rpi.file_id = if.id '
            'where if.filename = \'{0}\' or if.filename = \'{1}\'').format(filename1, filename2)
        self.cursor.execute(command)
        return self.cursor.fetchall()

    def upsert_region_period_indicators(self, region_id, indicator_id, period_id, value):
        command = (
            'INSERT INTO data.region_period_indicators'
            '(region_id, indicator_id, period_id, value, file_id) '
            'VALUES ({0}, {1}, {2}, {3}, null) '
            'ON CONFLICT(region_id, indicator_id, period_id, file_id) '
            'where file_id is null'
            'do UPDATE SET value = EXCLUDED.value '
        ).format(region_id, indicator_id, period_id, value)
        self.execute_query(command)

    def select_indicator_id(self, code):
        command = ('select id from data.indicators '
                   'where code = \'{0}\'').format(code)
        indicator = self.select(command)
        return indicator[0]

    def select_data_by_indicators(self, indicator1, indicator2, region):
        command = ('select rpi.value, p.id '
                   'from data.region_period_indicators rpi '
                   'join data.periods p on rpi.period_id = p.id '
                   'where rpi.indicator_id = {0} or rpi.indicator_id = {1} '
                   'and rpi.region_id = {2} '
                   'order by p.value').format(indicator1, indicator2, region)
        data = self.select(command)
        return data

    def select_data_by_indicator(self, indicator, region):
        command = ('select rpi.value, p.id '
                   'from data.region_period_indicators rpi '
                   'join data.periods p on rpi.period_id = p.id '
                   'where rpi.indicator_id = {0} '
                   'and rpi.region_id = {1} '
                   'order by p.value').format(indicator, region)
        data = self.select(command)
        return data

    def update_mom_region_period_indicators(self, mom, region_id, indicator_id, period_id):
        global cursor, connection
        command = ('UPDATE data.region_period_indicators '
                   'SET value = {0} '
                   'WHERE region_id = {1} and indicator_id = {2} '
                   'and period_id = {3}').format(mom, region_id, indicator_id, period_id)
        self.execute_query()

    def select_regions(self):
        command = ('select id, name '
                   'from data.regions')
        regions = self.select(command)
        return regions

    def select_period_id(self, date):
        command = "select id from data.periods where value = '{0}'".format(date)
        return self.select(command)[0]

    #Gulnaz
    def select_count_rules_checking(self):
        command = "SELECT COUNT(*) FROM data.rules_checking"
        return self.select(command)[0]

    #def select_ids_from_rules_checking(self, counter):
    #    command = "SELECT * FROM " \
    #              "(SELECT id, rule_id, period_id, region_id, indicator_id, " \
    #              "ROW_NUMBER() OVER(ORDER BY id) AS ROW " \
    #              "FROM data.rules_checking) AS TMP " \
    #              "WHERE ROW = '{0}'".format(counter);
    #    return self.select(command)

    def select_ids_from_rules_checking(self):
        command = "SELECT id, rule_id, period_id, region_id, indicator_id FROM data.rules_checking";
        self.cursor.execute(command)
        result = self.cursor.fetchall()
        return result

    def select_code_rule(self, id_rule):
        command = "SELECT code " \
                  "FROM data.rules " \
                  "WHERE id = '{0}'".format(id_rule)
        return self.select(command)[0]

    def select_value_from_regperindc(self, id_period, id_region, id_indicator):
        command = "SELECT value " \
                  "FROM data.region_period_indicators " \
                  "WHERE period_id = '{0}' AND region_id = '{1}' " \
                  "AND indicator_id = '{2}'".format(id_period, id_region, id_indicator)
        self.cursor.execute(command)
        result = self.cursor.fetchone()
        return result
    
    def check_status(self, id):
        command = "SELECT status FROM data.rules_checking WHERE id = '{0}'".format(id)
        return self.select(command)[0]

    def update_statusT_rules_check(self, id):
        if self.check_status(id) == False:
            command = "UPDATE data.rules_checking SET status = true WHERE id = {0}".format(id)
            self.execute_query(command)

    def update_statusF_rules_check(self, id):
        if self.check_status(id) == True:
            command = "UPDATE data.rules_checking " \
                      "SET status = false " \
                      "WHERE id = {0}".format(id)
            self.execute_query(command)

    def insert_data_logging(self, error, priority):
        error = str(error)
        error = error.replace('\'', '')
        command = "INSERT INTO data.logging(created_at_date, description, priority) " \
                  "VALUES (current_date,'{0}','{1}')".format(error, priority)
        self.execute_query(command)

    def select_id_from_rules(self, rule):
        command = "SELECT id FROM data.rules WHERE code = '{0}'".format(rule)
        return self.select(command)[0]

    def select_id_from_region(self, region):
        command = "SELECT id FROM data.regions WHERE name = '{0}' ".format(region)
        return self.select(command)[0]

    def select_id_from_indicator(self, code, filename):
        command = "SELECT id FROM data.indicators WHERE code = '{0}' AND xls_filename = '{1}'".format(code, filename)
        return self.select(command)[0]

    def select_id_from_period(self, period):
        command = "SELECT id FROM data.periods WHERE value_label = '{0}' ".format(period)
        return self.select(command)[0]

    def insert_data_rules_checking(self, rule, region, indicator, period):
        command = "INSERT INTO data.rules_checking(rule_id, region_id, indicator_id, period_id, status, created_at) " \
                  "VALUES ('{0}', '{1}', '{2}', '{3}', true, current_date) " \
                  "on conflict (rule_id, region_id, indicator_id, period_id) do nothing".format(
            rule, region, indicator, period)
        self.execute_query(command)

    def insert_data_rules(self, user_id, code, description_log):
        command = "INSERT INTO data.rules(user_id, code, description_log, flag) " \
                  "VALUES ('{0}', '{1}', '{2}', true)".format(
            user_id, code, description_log, flag)
        self.execute_query(command)

    def select_logs_from_logging(self, c_date):
        command = "SELECT created_at_date, description, priority " \
                  "FROM data.logging " \
                  "WHERE created_at_date = '{0}'".format(c_date)
        self.cursor.execute(command)
        result = self.cursor.fetchall()
        return result

    def messageForLoggingDQ(self, period_val, region_val, indicators_val, rule_val):
        message = "Обратите внимание! В периоде {0}, регионе {1} и индикаторе {2} {3}".format(
            period_val, region_val, indicators_val, rule_val
        )
        print(message)
        return message

    def exception_log(self, error):
        raise psycopg2.DatabaseError(error)

    def select_period_by_id(self, period_id):
        command = "SELECT value_label " \
                  "FROM data.periods " \
                  "WHERE id = '{0}'".format(period_id)
        return self.select(command)[0]

    def select_region_by_id(self, region_id):
        command = "SELECT name " \
                  "FROM data.regions " \
                  "WHERE id = '{0}'".format(region_id)
        return self.select(command)[0]

    def select_indicator_by_id(self, indicator_id):
        command = "SELECT name " \
                  "FROM data.indicators " \
                  "WHERE id = '{0}'".format(indicator_id)
        return self.select(command)[0]

    def select_rule_dscription_by_id(self, rule_id):
        command = "SELECT description_log " \
                  "FROM data.rules " \
                  "WHERE id = '{0}'".format(rule_id)
        return self.select(command)[0]
