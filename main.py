import tomskeedb as tsk
import numpy as np
import csv
from parser import Query
from table import Table
import pandas as pd


def prewiev():
    table = tsk.Table({'id': [1, 2, 3, 4, 5, 6, 7, 8],
                       'name': ['Albert', 'Bob', 'Claire', 'Kyle', 'Mon', 'William', 'Hovard', 'Kevin'],
                       'score': [19.2, 90, 92.3, 55.67, 93.4, 1.22, 99.3, 67.4]}, name='table_1')

    table.insert(data={'state': [1, 2, 3, 4, 5, 3, 6, 4], 'cash': [0.55, 0.76, 0.34, 0.44, 0.12, 0.33, 0.99, 0.87]},
                 axis=1)
    table.select(['name', 'id', 'id', 'cash'])
    table.insert([[1, 'Jim', None, 5, 6.23]], axis=0)
    table.drop(['score'], axis=1)
    table.select(columns=['id', 'name'], limit=2, offset=6)
    table.select()
    print(table)
    return table


def csv_prewiev():
    csv = r'D:\Projects\Python\database\test_csv\dota_hero_stats.csv'
    table = tsk.TomskeeDB.read_csv(csv)
    return table


if __name__ == '__main__':
    table = Table()
    print(table.shape, table.dtypes, table.columns, table._data)
    table.insert({'a': [1,2,3]}, axis=1)
    print(table.shape, table.dtypes, table.columns, table._data)
    table.select()
