import logging

import MySQLdb
from sqlalchemy import create_engine, inspect, NVARCHAR
from libs.common import environment as env


MYSQL_HOST = env.get_env_with_def('MYSQL_HOST', 'mariadb')
MYSQL_USER = env.get_env_with_def('MYSQL_USER', 'root')
MYSQL_PWD = env.get_env_with_def('MYSQL_PWD', 'mariadb')
MYSQL_SCHEMA = env.get_env_with_def('MYSQL_DB', 'stock_data')


def engine():
    return engine_to_db(MYSQL_SCHEMA)


def engine_to_db(to_db):
    mysql_url = "mysql+mysqldb://" + MYSQL_USER + ":" + MYSQL_PWD + "@" + MYSQL_HOST + "/" + to_db + "?charset=utf8"
    logging.info(f'Create engine for MySQL: {mysql_url}')
    return create_engine(
        mysql_url,
        encoding='utf8',
        convert_unicode=True
    )


def conn():
    db = MySQLdb.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PWD, MYSQL_SCHEMA, charset="utf8")
    db.autocommit(on=True)
    return db


def insert(sql, params=()):
    connection = conn()
    cursor = connection.cursor()
    logging.info(f'Executing insert statement [{sql}] with params [{params}]')
    try:
        cursor.execute(sql, params)
    except Exception as e:
        print("error :", e)
    finally:
        cursor.close()
        connection.close()


def select(sql, params=()):
    with conn() as db:
        print("select sql:" + sql)
        try:
            db.execute(sql, params)
        except Exception as e:
            print("error :", e)
        result = db.fetchall()
        return result


def insert_db(data, table_name, write_index, primary_keys):
    insert_other_db(MYSQL_SCHEMA, data, table_name, write_index, primary_keys)


def insert_other_db(to_db, data, table_name, write_index, primary_keys):
    # 定义engine
    engine_mysql = engine_to_db(to_db)
    # 使用 http://docs.sqlalchemy.org/en/latest/core/reflection.html
    # 使用检查检查数据库表是否有主键。
    insp = inspect(engine_mysql)
    col_name_list = data.columns.tolist()
    # 如果有索引，把索引增加到varchar上面。
    if write_index:
        # 插入到第一个位置：
        insert(0, data.index.name)
    print(col_name_list)
    data.to_sql(name=table_name, con=engine_mysql, schema=to_db, if_exists='append',
                dtype={col_name: NVARCHAR(length=255) for col_name in col_name_list}, index=write_index)
    # 判断是否存在主键
    if insp.get_primary_keys(table_name) == []:
        with engine_mysql.connect() as con:
            # 执行数据库插入数据。
            try:
                con.execute('ALTER TABLE `%s` ADD PRIMARY KEY (%s);' % (table_name, primary_keys))
            except Exception as e:
                logging.error()
                print("################## ADD PRIMARY KEY ERROR :", e)


class MySql:

    def __init__(self):
        self.host = env.get_env_with_def('MYSQL_HOST', 'mariadb')
        self.user = env.get_env_with_def('MYSQL_USER', 'root')
        self.password = env.get_env_with_def('MYSQL_PWD', 'mariadb')
        self.schema = env.get_env_with_def('MYSQL_DB', 'stock_data')

    def conn(self):
        db = MySQLdb.connect(self.host, self.user, self.password, self.schema, charset="utf8")
        db.autocommit(on=True)
        return db

    @staticmethod
    def count_by_date(table_name, date):
        return MySql.count_with_where_clause(table_name, "`date`= %s ", [date])

    @staticmethod
    def count_with_where_clause(table_name, clause, params=()):
        sql_count = " SELECT count(1) FROM `stock_data`.`" + table_name + "` WHERE " + clause
        count = MySql.select_count(sql_count, params)
        return count

    @staticmethod
    def del_by_date(table_name, date):
        try:
            del_sql = " DELETE FROM `stock_data`.`" + table_name + "` WHERE `date`= %s " % date
            insert(del_sql)
        except Exception as e:
            print("error :", e)

    @staticmethod
    def select_count(sql, params=()):
        connection = conn()
        cursor = connection.cursor()
        logging.info(f'Select count with sql{sql} and params[{params}]')
        try:
            cursor.execute(sql, params)
            result = cursor.fetchall()
            if len(result) == 1:
                return int(result[0][0])
            else:
                return 0
        except Exception as e:
            logging.exception('Execute select count statement error')
        finally:
            cursor.close()
            connection.close()
