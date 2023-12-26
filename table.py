"""
Provide access to data, stored in table of TomskeeDB.

"""
from typing import List, Dict, Union, Any

import numpy as np
from tabulate import tabulate

import tomskeedb as tsk
from exceptions import TDB_Exception, ValidationException


class Table:

    def __init__(self, data: Union[Dict[Any, Any], List[Any], np.ndarray] = None,
                 columns: List[str] = None,
                 dtypes: List[str] = None):
        if data is None:
            return
        (data_, columns_) = tsk.transform(data, columns)
        tsk.TomskeeDB.validate_init_table(data_, columns_, dtypes)
        self.columns = np.array(columns_) if columns_ else Table._create_columns(len(data[0]))
        self.shape = (len(data_), len(self.columns))
        self.dtypes = self._create_dtypes_(dtypes) if dtypes else self._define_dtypes(data_)
        self._data = self._create_data(data_)

    @staticmethod
    def _create_columns(self, n):
        return np.array([f'Unnamed: {i}' for i in range(n)])

    def _create_dtypes_(self, dtypes) -> dict:
        return {col: dtype for col, dtype in zip(self.columns, dtypes)}

    def _define_dtypes(self, data) -> dict:
        dtypes = {}
        for i in range(self.shape[1]):
            dt = None
            for j in range(len(data)):
                if data[j][i] is None and dt is int:
                    dt = float
                    break
                dt = type(data[j][i])
            dtypes[self.columns[i]] = dt
        return dtypes

    def _create_data(self, data) -> dict:
        temp_data = {}
        for i in range(self.shape[1]):
            for j, col in enumerate(self.columns):
                dt = self.dtypes[col]
                temp_data[col] = np.array([x[j] for x in data], dtype=dt)
        return temp_data


    def update_shape(self):
        self.shape = (len(next(iter(self._data.values()))), len(self.columns))

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
        self.dtypes = {col: type(dtype[0]) for col, dtype in zip(self.columns, self._data.values())}

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
                table_data[table_idx][j] = self._data[col][i]
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
                table_data[table_idx][j] = self._data[col][i]
            table_idx += 1
        print(tabulate(table_data, headers=columns, tablefmt="pretty"))

    def insert(self, data: Union[Dict[str, Any], List[List[Any]], np.ndarray],
               axis: int = 0,
               columns: List['str'] = None,
               dtypes: List['str'] = None):
        tsk.TomskeeDB.validate_insert_data(data, columns)
        dt = []
        if dtypes is not None:
            tsk.TomskeeDB._validate_dtypes(dtypes)
            dt = dtypes
        else:
            dt = self.define_dtypes_from_insert(data, columns)

        if isinstance(data, dict):
            for i, key in enumerate(data.keys()):
                dtype = dt[key]
                self._data[key] = np.array(data[key], dtype=dtype)
            self.update_columns(data)
            self.update_shape()
            self.update_dtypes()

        elif isinstance(data, list) or type(data) is np.ndarray:
            if columns is None:
                raise TDB_Exception('You have to implicitly declare column names')
            for i, col in enumerate(columns):
                dtype = dt[col]
                self._data[col] = np.array(data[i], dtype=dtype)
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
                del (self._data[col])
            self.delete_columns(columns)
            self.update_shape()
            self.update_dtypes()
        elif index is not None:
            if isinstance(index, range):
                index = [index.start, index.stop]
            else:
                index = [index, index + 1]
            for col in self.columns:
                self._data[col] = np.delete(self._data[col], [x for x in range(index[0], index[1])])
            self.update_shape()

    def debug(self):
        print(self._data)
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
