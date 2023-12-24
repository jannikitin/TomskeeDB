"""

TomskeeDB is my pet project. It's open-source project of DBMS that supports SQL quering and
simple interface to communicate with DBMS

"""

import numpy as np
from typing import List, Final, Union
import csv
import os
from Exceptions import TDB_Exception, ValidationException
from table import Table
import Schema


class TomskeeDB:
    dtypes = {'int', 'float', 'str', 'numpy.array', int, float, str, np.ndarray}
    COLUMN_TITLE_SIZE_: Final = 128

    @classmethod
    def read_csv(cls, PATH: str, dtypes=None):
        if os.path.basename(PATH) not in os.listdir(os.path.dirname(PATH)):
            raise ValidationException(f'{os.path.basename(PATH)} not found in directory')
        with open(PATH) as csv_file:
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
    def validate_dtypes(cls, columns, dtypes: List[str]) -> None:
        for dtype in dtypes:
            if dtype not in cls.dtypes:
                raise ValidationException(f'{dtype} is incorrect data type')

    @classmethod
    def validate_data(cls, data, columns) -> None:
        if isinstance(data, dict):
            primary_size = len(next(iter(data.values())))
            for key in data.keys():
                if len(key) > cls.COLUMN_TITLE_SIZE_:
                    raise TDB_Exception(f'Column title {key} is too big (max size: {cls.COLUMN_TITLE_SIZE_}')
                if len(data[key]) != primary_size:
                    raise ValidationException('Columns must be same size')

        elif isinstance(data, list) or type(data) is np.ndarray:
            if columns is not None:
                for i in range(len(data)):
                    if len(columns) != len(data[i]):
                        raise ValidationException('Length of column names must be the same as length of data')
                for column in columns:
                    if len(column) > cls.COLUMN_TITLE_SIZE_:
                        raise TDB_Exception(f'Column title {column} is too big (max size: {cls.COLUMN_TITLE_SIZE_}')
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
        TomskeeDB.validate_data(data, columns)
        if dtypes:
            TomskeeDB.validate_dtypes(columns, dtypes)
