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

from exceptions import TableConsistencyExeption, ValidationError
from table import Table
from schema import Schema
from parser import Parser, Query

__version__ = '1.1.3'
HELPER_TXT = 'data/help.txt'
SQL_TXT = 'data/sql.txt'
_COLUMN_TITLE_SIZE: Final = 128


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
    with open('data/help.txt', 'r') as f:
        print(f.read())


def sql_helper():
    with open('data/sql.txt', 'r') as f:
        print(f.read())


def SQL_QUERY():
    current_schema = Schema()
    csv = r'D:\Projects\Python\database\test_csv\dota_hero_stats.csv'

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
                except TableConsistencyExeption as e:
                    print('Incorrect operation')
                try:
                    current_schema.execute(query)
                except TableConsistencyExeption:
                    print('R U idiot?')


def read_csv(path: str, dtypes=None) -> Table:
    if os.path.basename(path) not in os.listdir(os.path.dirname(path)):
        raise ValidationError(f'{os.path.basename(path)} not found in directory')
    with open(path) as csv_file:
        csv_reader = csv.reader(csv_file)
        columns = next(csv_reader)
        data = []
        for line in csv_reader:
            data.append(line)
        return Table(data, columns, dtypes, name=os.path.basename(path).split('.')[0])


def columns_validation(columns=None) -> None:
    for column in columns:
        if len(column) > _COLUMN_TITLE_SIZE:
            raise TableConsistencyExeption(f'Column title {column} is too big (max size: {_COLUMN_TITLE_SIZE}')
        if not isinstance(column, str):
            raise ValidationError(f'Name error: type {type(column)} is unsupported type for column name. Column '
                                  f'name must be of "str" type')


def set_dtype(dtype=None):
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
