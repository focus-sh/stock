import logging
from functools import reduce

import MySQLdb
from sqlalchemy import create_engine, inspect
from lib.environment import environment as env


class MySql:

    def __init__(self):
        self.host = env.get_env_with_def('MYSQL_HOST', 'mariadb')
        self.user = env.get_env_with_def('MYSQL_USER', 'root')
        self.password = env.get_env_with_def('MYSQL_PWD', 'mariadb')
        self.schema = env.get_env_with_def('MYSQL_DB', 'stock_data')

    def create_new_schema_if_necessary(self):
        [connection, cursor] = [None, None]
        try:
            connection = MySQLdb.connect(
                mysql.host,
                mysql.user,
                mysql.password,
                mysql.schema,
                charset="utf8"
            )

            cursor = connection.cursor()
            cursor.execute(" paged_select 1 ")
        except Exception:
            logging.info(f"The schema[{mysql.schema}] doesnt exist, create a new one.")
            self.create_new_schema(self.schema)
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    def create_new_schema(self, schema):
        [connection, cursor] = [None, None]
        try:
            connection = MySQLdb.connect(self.host, self.user, self.password, charset="utf8")
            cursor = connection.cursor()
            create_sql = " CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8 COLLATE utf8_general_ci " % schema
            cursor.execute(create_sql)
        except Exception:
            logging.exception(f'Create schema[{schema}] failed.')
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    def connection(self):
        db = MySQLdb.connect(self.host, self.user, self.password, self.schema, charset="utf8")
        db.autocommit(on=True)
        return db

    def insert(self, sql, params=()):
        connection = self.connection()
        cursor = connection.cursor()
        logging.info(f'Executing insert statement [{sql}] with params [{params}]')
        try:
            cursor.execute(sql, params)
        except Exception:
            logging.exception(f'Executing insert statement [{sql}] with params [{params}] failed')
        finally:
            cursor.close()
            connection.close()

    def insert_db(self, data, table_name, primary_keys):
        self.insert_other_db(self.schema, data, table_name, primary_keys)

    def insert_other_db(self, to_db, data, table_name, primary_keys):
        data = data.round(4)  # 入库数据中浮点数，统一保留4位小数
        engine_mysql = self.engine_to_db(to_db)
        data.to_sql(name=table_name, con=engine_mysql, if_exists='append', index=(data.index.name is not None))
        if not inspect(engine_mysql).get_primary_keys(table_name):
            with engine_mysql.connect() as con:
                try:
                    con.execute(
                        'ALTER TABLE `%s` ADD PRIMARY KEY (%s);' % (table_name, self.concat_list_params(primary_keys)))
                except Exception as e:
                    logging.exception('Insert data into database error.')

    def engine(self):
        return self.engine_to_db(self.schema)

    def engine_to_db(self, to_db):
        mysql_url = "mysql+mysqldb://" + self.user + ":" + self.password + "@" + self.host + "/" + to_db + "?charset=utf8"
        return create_engine(
            mysql_url,
            encoding='utf8',
            convert_unicode=True
        )

    def count_by_date(self, table_name, date):
        return self.count_with_where_clause(table_name, "where `date`= %s ", [date])

    def count_with_where_clause(self, table_name, clause, params=[]):
        sql = self.count_sql(table_name, clause)
        connection = self.connection()
        cursor = connection.cursor()
        logging.info(f'Select count with sql<{sql}> and params<{params}>')
        try:
            cursor.execute(sql, params)
            result = cursor.fetchall()
            if len(result) == 1:
                return int(result[0][0])
            else:
                return 0
        except MySQLdb.ProgrammingError:
            logging.exception('Execute paged_select count statement error')
            return 0
        finally:
            cursor.close()
            connection.close()

    def select(self, sql, params=()):
        with self.connection() as db:
            logging.info(f'Select data by sql<{sql}> and params<{params}>')
            try:
                db.execute(sql, params)
            except Exception:
                logging.exception(f'Select data by sql[{sql}] and params[{params}] failed')
            result = db.fetchall()
            return result

    def del_by_date(self, table_name, date):
        try:
            del_sql = " DELETE FROM `stock_data`.`" + table_name + "` WHERE `date`= %s " % date.strftime("%Y%m%d")
            self.insert(del_sql)
        except Exception as e:
            print("error :", e)

    @staticmethod
    def count_sql(table_name, where_clause):
            return f"""
    SELECT 
        COUNT(1)
    FROM
        `stock_data`.{table_name}
    {where_clause}"""

    @staticmethod
    def concat_list_params(lst):
        if isinstance(lst, str):
            return f'`{lst}`'

        return reduce(lambda x, y: f'{x}, `{y}`', lst, '')[2:]



mysql = MySql()
