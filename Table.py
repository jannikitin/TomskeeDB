"""
Provide access to data, stored in table of TomskeeDB.
"""

import TomskeeDB as tdb
import numpy as np
from typing import List, Dict, Union
from tabulate import tabulate
import ast
from Exceptions import TDB_Exception, ValidationException


class Table:

    def __init__(self, data: Union[Dict, List, np.ndarray] = None,
                 columns: List[str] = None,
                 dtypes: List[str] = None, **kwargs):

        self.data = {}
        self.shape = tuple()
        self.dtypes = np.array([])
        self.columns_ = np.array([])
        self.raw_data_ = np.array([])
        if data is None and columns is None:
            return
        tdb.TomskeeDB.validate_init_table(data, columns, dtypes)

        self.dtypes = np.array(dtypes)
        self.create_shape(data, columns)
        self.shape = (len(self.raw_data_), len(self.columns_))
        self.create_data()

    def create_shape(self, data, columns):
        if isinstance(data, dict):
            self.table_from_dict(data)
        elif isinstance(data, list):
            self.table_from_list(data, columns)
        elif type(data) is np.ndarray:
            self.table_from_numpy(data, columns)
        else:
            raise TDB_Exception(f'{type(data)} is incorrect dtype')

    def table_from_dict(self, data):
        self.columns_ = np.array([x for x in data.keys()])
        self.raw_data_ = np.array([x for x in data.values()])
        for i, col in enumerate(self.columns_):
            self.data[col] = np.array(self.raw_data_[i])

    def table_from_list(self, data, columns):
        if columns:
            self.raw_data_ = np.array([x for x in data])
            self.columns_ = np.array([x for x in columns])
        else:
            self.columns_ = np.array([col for col in range(0, len(data[0]))])
            self.raw_data_ = np.array([x for x in data])

    def table_from_numpy(self, data, columns):
        if columns:
            self.raw_data_ = data
            self.columns_ = np.array(columns)
        else:
            self.raw_data_ = data
            self.columns_ = np.array([x for x in range(len(data))])

    def create_data(self):
        if self.data is True:
            return
        for i, col in enumerate(self.columns_):
            self.data[col] = [x[i] for x in self.raw_data_]
        if self.dtypes is not None:
            for i, key in enumerate(self.data.keys()):
                dtype = tdb.TomskeeDB.set_dtype(self.dtypes[i])
                for j in range(len(self.data[key])):
                    if dtype is list:
                        self.data[key][j] = ast.literal_eval(self.data[key][j])
                    else:
                        self.data[key][i] = dtype(self.data[key][i])

    def get(self, columns: List[str] = '*', idx: Union[range, int] = None):
        if idx is None:
            idx = [0, self.shape[0]]
        elif isinstance(idx, range):
            idx = [idx.start, idx.stop]
        else:
            idx = [0, idx]

        if idx[1] > self.shape[0]:
            raise ValidationException(f'Index is out of range. Rows = {self.shape[0]}, your number of rows = {idx[1]}')

        if columns == '*':
            columns = self.columns_
        else:
            for col in columns:
                if col not in self.columns_:
                    raise ValidationException(f'{col} not found')

        table_data = [[0 for _ in range(len(columns))] for _ in range(idx[1] - idx[0])]
        table_idx = 0
        for i in range(idx[0], idx[1]):
            for j, col in enumerate(columns):
                table_data[table_idx][j] = self.data[col][i]
            table_idx +=1
        print(tabulate(table_data, headers=columns, tablefmt="pretty"))

    def print(self):
        print(self.data)

    @staticmethod
    def valid_input(cols, data):
        return len(cols) == len(data[0])
