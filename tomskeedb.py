"""
TomskeeDB is my pet project. It's open-source project of DBMS that supports SQL quering and
simple interface to communicate with DBMS

"""
import csv
import os
import time
from typing import List, Final, Union

import numpy as np
import sqlparse

from exceptions import TskException, ValidationException
from table import Table
from schema import Schema
from parser import Parser,  Query
__version__ = '1.1.0'


def display():
    print(f'TOMSKEEDB {__version__}')
    print('Type \\h for help window.')
    while True:
        action = input('>>')
        match action:
            case 'q':
                print('Closing TomskeeDB')
                time.sleep(1)
                break
            case r'\h':
                tsk_helper()
            case r'\sql':
                sql_helper()
            case r'\run':
                SQL_QUERY()
            case 'cls':
                os.system('cls')
            case _:
                print(f'{action} command is unknown')


def tsk_helper():
    with open('help.txt', 'r') as f:
        print(f.read())


def sql_helper():
    with open('sql.txt', 'r') as f:
        print(f.read())


def SQL_QUERY():
    current_schema = Schema()
    csv = r'D:\Projects\Python\database\test_data\dota_hero_stats.csv'
    table = TomskeeDB.read_csv(csv)
    current_schema.create_table(table, name='table1')

    print('You can type "SELECT tsk.database_navigator" to learn, how to check your schemas and tables')
    while True:
        query = input('Type your SQL query here >>')
        match query:
            case 'q':
                print('Exiting SQL console...')
                time.sleep(1)
                break
            case 'cls':
                os.system('cls')
            case _:
                try:
                    query = Query(query)
                except TskException as e:
                    print('Incorrect operation')
                try:
                    current_schema.execute(query)
                except TskException:
                    print('R U idiot?')

def transform(data, columns=None):
    if isinstance(data, dict):
        columns_ = list(data.keys())
        data_ = []
        for i in range(len(next(iter(data.values())))):
            row = []
            for col in data.keys():
                try:
                    row.append(data[col][i])
                except KeyError:
                    raise ValidationException('Data must be the same size')
            data_.append(row)
        return data_, columns_
    elif isinstance(data, list) or isinstance(data, np.ndarray):
        if columns is None:
            return data, None
        return data, columns
    else:
        raise ValidationException(f'tsk cannot convert {type(data)} to Table')


class TomskeeDB:
    dtypes_ = {'int', 'float', 'str', 'numpy.ndarray', int, float, str, np.ndarray}
    COLUMN_TITLE_SIZE: Final = 128

    @classmethod
    def read_csv(cls, path: str, dtypes=None) -> Table:
        if os.path.basename(path) not in os.listdir(os.path.dirname(path)):
            raise ValidationException(f'{os.path.basename(path)} not found in directory')
        with open(path) as csv_file:
            csv_reader = csv.reader(csv_file)
            columns = next(csv_reader)
            data = []
            for line in csv_reader:
                data.append(line)
            return Table(data, columns, dtypes, name=os.path.basename(path).split('.')[0])

    @classmethod
    def to_csv(cls, FILE_NAME: str):
        pass

    @classmethod
    def validate_dtypes(cls, dtypes: List[str]) -> None:
        for dtype in dtypes:
            if dtype not in cls.dtypes_:
                raise ValidationException(f'{dtype} is unsupported data type')

    @classmethod
    def columns_validation(cls, data, columns=None) -> None:
        if columns:
            for column in columns:
                if len(column) > cls.COLUMN_TITLE_SIZE:
                    raise TskException(f'Column title {column} is too big (max size: {cls.COLUMN_TITLE_SIZE}')

    @classmethod
    def set_dtype(cls, data=None, dtype=None):
        match dtype:
            case 'int':
                return int
            case 'float':
                return float
            case 'str':
                return str
            case 'list':
                return list
            case 'numpy.ndarray':
                return np.ndarray
            case _:
                return

    @classmethod
    def validate_init_table(cls, data, columns, dtypes):
        cls.columns_validation(data, columns)
        if dtypes:
            cls.validate_dtypes(dtypes)
