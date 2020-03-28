import unittest

from sqlalchemy import create_engine

from lib.mysql import mysql
import pandas as pd


class TestMySql(unittest.TestCase):

    def test_should_drop_duplicates(self):
        self.assertTrue(mysql.should_drop_duplicates(None, None))
        self.assertTrue(mysql.should_drop_duplicates(None, []))
        self.assertFalse(mysql.should_drop_duplicates("code", "code"))
        self.assertFalse(mysql.should_drop_duplicates("code", ["code", "date"]))
        self.assertTrue(mysql.should_drop_duplicates("code", ["id", "man"]))

    def test_concat_list_params(self):
        arr = ['a', 'b', 'c', 'd']
        self.assertEqual(mysql.concat_list_params(arr), '`a`, `b`, `c`, `d`')

        arr = 'code'
        self.assertEqual(mysql.concat_list_params(arr), '`code`')

    @unittest.skip
    def test_insert_into_db(self):
        data = pd.DataFrame(
            {
                'code': ["000001"],
                'date': ['20200101']
            }
        )
        mysql.insert(data, 'test_table', ['code', 'date'])

    def test_create_mysql_engin(self):
        engine_mysql = create_engine(
            "mysql+mysqldb://root:mariadb@mariadb",
            encoding='utf8',
            convert_unicode=True
        )
        self.assertIsNotNone(engine_mysql)

        sql = 'CREATE DATABASE IF NOT EXISTS test_db CHARACTER SET utf8 COLLATE utf8_general_ci'
        with engine_mysql.connect() as connect:
            connect.execute(sql)
