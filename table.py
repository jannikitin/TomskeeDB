"""
Provide access to data, stored in table of TomskeeDB.
"""

import tomskeedb as tsk
import numpy as np
from typing import List, Dict, Union, Any
from tabulate import tabulate
from Exceptions import TDB_Exception, ValidationException


class Table:

    def __init__(self, data: Union[Dict[Any, Any], List[Any], np.ndarray] = None,
                 columns: List[str] = None,
                 dtypes: Union[Dict[str, Any], List[str]] = None):
        if data is None:
            return
        tsk.TomskeeDB.validate_init_table(data, columns, dtypes)
        self.columns = self.create_columns_(data, columns)
        self.shape = self.create_shape_(data)
        if dtypes:
            self.dtypes = self.create_dtypes_(dtypes)
        else:
            self.dtypes = self.define_dtypes_(data)
        self.data_ = self.create_data_(data)

    @staticmethod
    def create_columns_(data: Union[Dict[Any, Any], List[Any], np.ndarray], columns: List[str]) -> np.ndarray:
        if isinstance(data, dict):
            return np.array([x for x in data.keys()])
        elif columns is not None:
            return np.array(columns)
        else:
            return np.array([f'Unnamed {n}' for n in range(len(data[0]))])

    def create_shape_(self, data: Union[Dict[Any, Any], List[Any], np.ndarray]) -> tuple:
        if isinstance(data, dict):
            return len(next(iter(data.values()))), len(self.columns)
        else:
            return len(data), len(self.columns)

    def create_dtypes_(self, dtypes: Union[Dict[str, Any]]) -> dict:
        dt = {}
        for i, col in enumerate(self.columns):
            dt[col] = tsk.TomskeeDB.set_dtype(dtypes[i])
        return dt

    def define_dtypes_(self, data: Union[Dict[Any, Any], List[Any], np.ndarray]) -> dict:
        dt = {}
        if isinstance(data, dict):
            for key in self.columns:
                dt[key] = type(data[key][0])
        else:
            for key in self.columns:
                dt[key] = type(data[0])
        return dt

    def create_data_(self, data: Union[Dict[Any, Any], List[Any], np.ndarray]) -> dict:
        if isinstance(data, dict):
            return self.table_from_dict_(data)
        elif isinstance(data, list) or type(data) is np.ndarray:
            return self.table_from_list_(data)
        else:
            raise TDB_Exception(f'{type(data)} is incorrect dtype')

    def table_from_dict_(self, data: Dict[Any, Any]) -> dict:
        temp_data = {}
        for i, col in enumerate(data.keys()):
            dt = self.dtypes[col]
            temp_data[col] = np.array(data[col], dtype=dt)
        return temp_data

    def table_from_list_(self, data: List[Any]) -> dict:
        temp_data = {}
        for i in range(self.shape[1]):
            for j, col in enumerate(self.columns):
                dt = self.dtypes[col]
                temp_data[col] = np.array([x[j] for x in data], dtype=dt)
        return temp_data

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
            columns = self.columns
        else:
            for col in columns:
                if col not in self.columns:
                    raise ValidationException(f'{col} not found')

        table_data = [[0 for _ in range(len(columns))] for _ in range(idx[1] - idx[0])]
        table_idx = 0
        for i in range(idx[0], idx[1]):
            for j, col in enumerate(columns):
                table_data[table_idx][j] = self.data_[col][i]
            table_idx += 1
        return Table(table_data, columns)

    def select(self, columns: List[str] = '*', idx: Union[range, int] = None):
        if idx is None:
            idx = [0, self.shape[0]]
        elif isinstance(idx, range):
            idx = [idx.start, idx.stop]
        else:
            idx = [0, idx]

        if idx[1] > self.shape[0]:
            raise ValidationException(f'Index is out of range. Rows = {self.shape[0]}, your number of rows = {idx[1]}')

        if columns == '*':
            columns = self.columns
        else:
            for col in columns:
                if col not in self.columns:
                    raise ValidationException(f'{col} not found')

        table_data = [[0 for _ in range(len(columns))] for _ in range(idx[1] - idx[0])]
        table_idx = 0
        for i in range(idx[0], idx[1]):
            for j, col in enumerate(columns):
                table_data[table_idx][j] = self.data_[col][i]
            table_idx += 1
        print(tabulate(table_data, headers=columns, tablefmt="pretty"))
