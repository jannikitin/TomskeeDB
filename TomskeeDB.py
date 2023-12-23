"""

TomskeeDB is my pet project. It's open-source project of DBMS that supports SQL quering and
simple interface to communicate with DBMS

"""

import numpy as np
from typing import List, Final
import csv
import os
from Exceptions import TDB_Exception, ValidationException
from Table import Table
import Schema


class TomskeeDB:
    dtypes = {'int', 'float', 'str', 'object', 'list', 'numpy.array'}
    COLUMN_TITLE_SIZE: Final = 128

    @classmethod
    def read_csv(cls, PATH: str, dtypes=None):
        if os.path.basename(PATH) not in os.listdir(os.path.dirname(PATH)):
            raise ValidationException(f'{os.path.basename(PATH)} not found in directory')
        with open(PATH) as csv_file:
            csv_reader = csv.reader(csv_file)
            columns = next(csv_reader)
            raw_data = []
            for line in csv_reader:
                raw_data.append(line)
            return Table(raw_data, columns, dtypes)

    @classmethod
    def to_csv(cls, FILE_NAME: str):
        pass

    @classmethod
    def validate_dtypes(cls, dtypes: List[str]) -> tuple:
        for dtype in dtypes:
            if dtype not in cls.dtypes:
                return False, dtype
        return True, 0

    @classmethod
    def validate_data(cls, data, columns):
        if isinstance(data, dict):
            primary_size = len(*(data.values()))
            for key in data.keys():
                if len(key) > TomskeeDB.COLUMN_TITLE_SIZE:
                    TDB_Exception(f'Column title {key} is too big (max size: {TomskeeDB.COLUMN_TITLE_SIZE}')
                elif len(data[key]) != primary_size:
                    ValidationException('Columns must be same size')
        elif isinstance(data, list) or type(data) is np.ndarray:
            if columns:
                if len(columns) != len(data[0]):
                    raise ValidationException('Length of column names must be the same as length of data')
                for column in columns:
                    if len(column) > TomskeeDB.COLUMN_TITLE_SIZE:
                        TDB_Exception(f'Column title {column} is too big (max size: {TomskeeDB.COLUMN_TITLE_SIZE}')
        else:
            raise ValidationException(f'TomskeeDB cannot convert {type(data)} into table')

    @classmethod
    def set_dtype(cls, dtype):
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

    @staticmethod
    def validate_init_table(data, columns, dtypes):
        if dtypes:
            dvalidation = TomskeeDB.validate_dtypes(dtypes)
            if not dvalidation[0]:
                raise ValidationException(f'{dvalidation[1]} is incorrect data type')
        TomskeeDB.validate_data(data, columns)
