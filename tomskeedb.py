"""
TomskeeDB is my pet project. It's open-source project of DBMS that supports SQL quering and
simple interface to communicate with DBMS

"""
import csv
import os
import time
from typing import List, Final, Union

import numpy as np

from exceptions import TDB_Exception, ValidationException
from table import Table
import schema
__version__ = '1.0.2'


def run(table):
    print(f'TOMSKEEDB {__version__}')
    while True:
        action = input('Print your SQL query here >> ')
        if action == 'q':
            print('Closing TomskeeDB')
            time.sleep(1)
            break
        if action == 'SELECT':
            table.select()


def transform(data, columns):
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
    dtypes_ = {'int', 'float', 'str', 'numpy.array', int, float, str, np.ndarray}
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
            return Table(data, columns, dtypes)

    @classmethod
    def to_csv(cls, FILE_NAME: str):
        pass

    @classmethod
    def _validate_dtypes(cls, columns, dtypes: List[str]) -> None:
        for dtype in dtypes:
            if dtype not in cls.dtypes_:
                raise ValidationException(f'{dtype} is unsupported data type')

    @classmethod
    def _data_validation(cls, data, columns=None) -> None:
        if columns:
            for column in columns:
                if len(column) > cls.COLUMN_TITLE_SIZE:
                    raise TDB_Exception(f'Column title {column} is too big (max size: {cls.COLUMN_TITLE_SIZE}')

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
            case 'numpy.array':
                return np.ndarray
            case _:
                return

    @classmethod
    def validate_init_table(cls, data, columns, dtypes):
        cls._data_validation(data, columns)
        if dtypes:
            cls._validate_dtypes(columns, dtypes)

