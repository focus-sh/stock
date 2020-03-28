import logging
from functools import reduce

from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import OperationalError, ProgrammingError

from lib.environment import environment as env


class MySql:

    def __init__(self):
        self.host = env.get_env_with_def('MYSQL_HOST', 'mariadb')
        self.user = env.get_env_with_def('MYSQL_USER', 'root')
        self.password = env.get_env_with_def('MYSQL_PWD', 'mariadb')
        self.schema = env.get_env_with_def('MYSQL_DB', 'stock_data')
        self.url = f"mysql+mysqldb://{self.user}:{self.password}@{self.host}/{self.schema}?charset=utf8"
        self.create_schema()

        self.add_primary_key_sql = 'ALTER TABLE `%s` ADD PRIMARY KEY (%s);'
        self.del_sql = "DELETE FROM `stock_data`.`%s`"
        self.count_sql = "SELECT COUNT(1) FROM `stock_data`.`%s`"

    def create_schema(self):
        logging.info(f'Create schema {self.schema} is necessary.')
        url = f"mysql+mysqldb://{self.user}:{self.password}@{self.host}"
        engine = self.engine(url)
        sql = f'CREATE DATABASE IF NOT EXISTS {self.schema} CHARACTER SET utf8 COLLATE utf8_general_ci'
        try:
            with engine.connect() as conn:
                conn.execute(sql)
        except OperationalError:
            logging.exception(f'Create schema {self.schema} failed. Maybe the database service is not ready.')

    def engine(self, url=None):
        return create_engine(url or self.url, encoding='utf8', convert_unicode=True)

    def insert(self, data, table_name, primary_keys, indexes=[]):
        #  数据去重
        if self.should_drop_duplicates(data.index.name, primary_keys):
            data = data.drop_duplicates(subset=primary_keys, keep="last")
        data = data.round(4)  # 数据精度修剪
        # 保存数据
        data.to_sql(name=table_name, con=self.engine(), if_exists='append', index=(data.index.name is not None))
        # 调整主键（若需要）
        self.add_primary_key(table_name, primary_keys)

    @staticmethod
    def should_drop_duplicates(index_name, primary_keys):
        if index_name is None:
            return True

        if isinstance(primary_keys, str):
            return primary_keys != index_name

        return index_name not in primary_keys

    def add_primary_key(self, table_name, primary_keys):
        engine = self.engine()
        if inspect(engine).get_pk_constraint(table_name)['constrained_columns']:
            return  # 已经包含主键，不再处理

        # 增加主键
        with engine.connect() as con:
            con.execute(self.add_primary_key_sql % (table_name, self.concat_list_params(primary_keys)))

    def concat_list_params(self, lst):
        if isinstance(lst, str):
            return f'`{lst}`'

        return reduce(lambda x, y: f'{x}, `{y}`', lst, '')[2:]

    def del_by_date(self, table_name, date):
        where_clause = " WHERE `date`= %s "
        self.del_with_where_clause(table_name, where_clause, [date.strftime("%Y%m%d")])

    def del_with_where_clause(self, table_name, clause, params=[]):
        sql = (self.del_sql % table_name) + clause
        self.execute_with_where_clause(sql, params)

    def count_by_date(self, table_name, date):
        return self.count_with_where_clause(table_name, " WHERE `date`= %s ", [date.strftime("%Y%m%d")])

    def count_with_where_clause(self, table_name, clause, params=[]):
        sql = (self.count_sql % table_name) + clause
        result = self.execute_with_where_clause(sql, params)
        return result.fetchone()[0]

    def execute_with_where_clause(self, sql, params=[]):
        with self.engine().connect() as con:
            try:
                return con.execute(sql, params)
            except ProgrammingError:
                logging.exception(f'Failed to execute sql<{sql}> with params<{params}>')


mysql = MySql()
