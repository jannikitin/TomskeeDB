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
        self.columns = self.__create_columns(data, columns)
        self.shape = self.__create_shape(data)
        self.dtypes = self.__create_dtypes_(data, dtypes)
        self.data_ = self.__create_data(data)

    # __INIT__ BLOCK
    @staticmethod
    def __create_columns(data: Union[Dict[Any, Any], List[Any], np.ndarray], columns: List[str]) -> np.ndarray:
        if isinstance(data, dict):
            return np.array([x for x in data.keys()])
        elif columns is not None:
            return np.array(columns)
        else:
            return np.array([f'Unnamed {n}' for n in range(len(data[0]))])

    def __create_shape(self, data: Union[Dict[Any, Any], List[Any], np.ndarray]) -> tuple:
        if isinstance(data, dict):
            return len(next(iter(data.values()))), len(self.columns)
        else:
            return len(data), len(self.columns)

    def __create_dtypes_(self, data, dtypes: Union[Dict[str, Any]]) -> dict:
        if dtypes is not None:
            dt = {}
            for i, col in enumerate(self.columns):
                dt[col] = tsk.TomskeeDB.set_dtype(dtype=dtypes[i])
            return dt
        else:
            return self.__define_dtypes(data=data)

    def __define_dtypes(self, data: Union[Dict[Any, Any], List[Any], np.ndarray]) -> dict:
        dt = {}
        if isinstance(data, dict):
            for key in self.columns:
                dt[key] = type(data[key][0])
        else:
            for key in self.columns:
                dt[key] = type(data[0])
        return dt

    def __create_data(self, data: Union[Dict[Any, Any], List[Any], np.ndarray]) -> dict:
        if isinstance(data, dict):
            return self.__table_from_dict(data)
        elif isinstance(data, list) or type(data) is np.ndarray:
            return self.__table_from_list(data)
        else:
            raise TDB_Exception(f'{type(data)} is incorrect dtype')

    def __table_from_dict(self, data: Dict[Any, Any]) -> dict:
        temp_data = {}
        for i, col in enumerate(data.keys()):
            dt = self.dtypes[col]
            temp_data[col] = np.array(data[col], dtype=dt)
        return temp_data

    def __table_from_list(self, data: List[Any]) -> dict:
        temp_data = {}
        for i in range(self.shape[1]):
            for j, col in enumerate(self.columns):
                dt = self.dtypes[col]
                temp_data[col] = np.array([x[j] for x in data], dtype=dt)
        return temp_data

    # __INIT__ BLOCK

    def update_shape(self):
        self.shape = (len(next(iter(self.data_.values()))), len(self.columns))

    def update_columns(self, data: Union[Dict[str, Any], List[str]]):
        if isinstance(data, dict):
            for col in data.keys():
                self.columns = np.append(self.columns, col)
        else:
            for col in data:
                self.columns = np.append(self.columns, col)

    def delete_columns(self, columns):
        for i, col in enumerate(self.columns):
            if col in columns:
                self.columns = np.delete(self.columns, i)

    def update_dtypes(self):
        self.dtypes = {col: type(dtype[0]) for col, dtype in zip(self.columns, self.data_.values())}

    @staticmethod
    def define_dtypes_from_insert(data, columns) -> dict:
        dt = {}
        if isinstance(data, dict):
            for i, key in enumerate(data.keys()):
                dt[key] = type(data[key][0])
        else:
            for i in range(len(data)):
                dt[columns[i]] = type(data[i][0])
        return dt

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

    def select(self, columns: List[str] = '*',
               limit: int = None,
               offset: int = None):
        index_range = self.index_ranger(limit, offset)

        if columns == '*':
            columns = self.columns
        else:
            for col in columns:
                if col not in self.columns:
                    raise ValidationException(f'{col} not found')

        table_data = [[0 for _ in range(len(columns))] for _ in range(index_range[1] - index_range[0])]
        table_idx = 0
        for i in range(index_range[0], index_range[1]):
            for j, col in enumerate(columns):
                table_data[table_idx][j] = self.data_[col][i]
            table_idx += 1
        print(tabulate(table_data, headers=columns, tablefmt="pretty"))

    def insert(self, data: Union[Dict[str, Any], List[List[Any]], np.ndarray],
               axis: int = 0,
               columns: List['str'] = None,
               dtypes: List['str'] = None):
        tsk.TomskeeDB.validate_insert_data(data, columns)
        dt = []
        if dtypes is not None:
            tsk.TomskeeDB.validate_dtypes(dtypes)
            dt = dtypes
        else:
            dt = self.define_dtypes_from_insert(data, columns)

        if isinstance(data, dict):
            for i, key in enumerate(data.keys()):
                dtype = dt[key]
                self.data_[key] = np.array(data[key], dtype=dtype)
            self.update_columns(data)
            self.update_shape()
            self.update_dtypes()

        elif isinstance(data, list) or type(data) is np.ndarray:
            if columns is None:
                raise TDB_Exception('You have to implicitly declare column names')
            for i, col in enumerate(columns):
                dtype = dt[col]
                self.data_[col] = np.array(data[i], dtype=dtype)
            self.update_columns(columns)
            self.update_shape()
            self.update_dtypes()

    def drop(self, columns: List[str] = None,
             index: Union[int, range] = None,
             axis: int = 0):
        if not index and not columns:
            raise TDB_Exception('Column and index are empty')
        if columns is not None:
            for col in columns:
                if col not in self.columns:
                    raise ValidationException(f'Cannot find {col} in axis')
            for col in columns:
                del (self.data_[col])
            self.delete_columns(columns)
            self.update_shape()
            self.update_dtypes()
        elif index is not None:
            if isinstance(index, range):
                index = [index.start, index.stop]
            else:
                index = [index, index + 1]
            for col in self.columns:
                self.data_[col] = np.delete(self.data_[col], [x for x in range(index[0], index[1])])
            self.update_shape()

    def debug(self):
        print(self.data_)
        print(self.columns)
        print(self.shape)
        print(self.dtypes)

    def index_ranger(self, limit, offset) -> tuple:
        rng = []
        if offset is None:
            rng.append(0)
        else:
            rng.append(offset)
        if limit is None:
            rng.append(self.shape[0])
        else:
            rng.append(rng[0] + limit)
        if rng[0] < 0 or rng[0] > self.shape[0] or rng[1] > self.shape[0] or rng[1] < 0:
            raise ValidationException(
                f'Index is out of range. Possible range = {self.shape[0]},'
                f' selected range = ({rng[0]}, {rng[1]})')
        return tuple(rng)
