"""
Provide access to data, stored in table of TomskeeDB.

"""
from typing import List, Dict, Union, Any

import numpy as np
from tabulate import tabulate

import tomskeedb as tsk
from exceptions import TskException, ValidationException


class Table:

    def __init__(self, data: Union[Dict[Any, Any], List[Any], np.ndarray] = None,
                 columns: List[str] = None,
                 dtypes: List[str] = None,
                 name: str = 'Unnamed'):
        if data is None:
            return
        (data_, columns_) = tsk.transform(data, columns)
        tsk.TomskeeDB.validate_init_table(data_, columns_, dtypes)
        self.columns = np.array(columns_) if columns_ else Table._create_columns(len(data[0]))
        self.shape = (len(data_), len(self.columns))
        self.dtypes = self._create_dtypes(dtypes) if dtypes else self._define_dtypes(data_)
        self._data = self._create_data(data_)
        self.name = name

    @staticmethod
    def _create_columns(n):
        return np.array([f'Unnamed: {i}' for i in range(n)])

    def _create_dtypes(self, dtypes) -> dict:
        return {col: dtype for col, dtype in zip(self.columns, dtypes)}

    def _define_dtypes(self, data, columns=None) -> dict:
        if not columns:
            columns = self.columns
        dtypes = {}
        for i in range(len(data[0])):
            dt = None
            for j in range(len(data)):
                if data[j][i] is None and dt is int:
                    dt = float
                    break
                dt = type(data[j][i])
            dtypes[columns[i]] = dt
        return dtypes

    def _create_data(self, data) -> dict:
        temp_data = {}
        for j, col in enumerate(self.columns):
            dt = self.dtypes[col]
            temp_data[col] = np.array([x[j] for x in data], dtype=dt)
        return temp_data

    def upload(self, columns=None, op='ins'):
        if columns:
            self.update_columns(columns, op)
        self.update_shape()
        self.update_dtypes()

    def update_shape(self):
        self.shape = (len(next(iter(self._data.values()))), len(self.columns))

    def update_columns(self, columns, op='ins'):
        if op == 'ins':
            self.columns = np.append(self.columns, columns)
        else:
            for i, col in enumerate(self.columns):
                if col in columns:
                    self.columns = np.delete(self.columns, i)

    def update_dtypes(self):
        self.dtypes = {col: type(dtype[0]) for col, dtype in zip(self.columns, self._data.values())}

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

        if columns == ['*']:
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

    def insert(self, data,
               axis: int = 0,
               columns: List[str] = None,
               dtypes: List[str] = None):
        if axis == 0:
            tsk.TomskeeDB.columns_validation(columns)
            if dtypes:
                tsk.TomskeeDB.validate_dtypes(dtypes)
            else:
                dtypes = None
            self._axis_0_insert(data)
        else:
            (tdata, tcolumns) = tsk.transform(data)
            if dtypes:
                tsk.TomskeeDB.validate_dtypes(dtypes)
            else:
                dtypes = self._define_dtypes(tdata, tcolumns)
            self._axis_1_insert(tdata, tcolumns, dtypes)

    def _axis_0_insert(self, data):
        for i, col in enumerate(self.columns):
            dt = self.dtypes[col]
            try:
                row = np.array([x[i] for x in data], dtype=dt)
            except Exception as e:
                raise e
            self._data[col] = np.append(self._data[col], row)
            self.upload()

    def _axis_1_insert(self, data, columns, dtypes):
        if len(data) != self.shape[0]:
            raise ValidationException('Wrong size')
        for i, col in enumerate(columns):
            self._data[col] = np.array([x[i] for x in data])
        self.upload(columns, 'ins')

    def _axis_0_dropper(self, index):
        for i in range(index[0], index[1]):
            for col in self.columns:
                self._data[col] = np.delete(self._data[col], i)
        self.upload()

    def _axis_1_dropper(self, columns):
        for col in columns:
            del(self._data[col])
        self.upload(columns, 'del')

    def drop(self, columns: List[str] = None,
             index: Union[int, range] = None,
             axis: int = 0):
        if not index and not columns:
            raise TskException('Column and index are empty')
        if axis == 0:
            index_ranger = [index, index] if isinstance(index, int) else [index[0], index[1]]
            self._axis_0_dropper(index_ranger)
        else:
            for col in columns:
                if col not in self.columns:
                    raise ValidationException(f'{col} not found in axis')
                self._axis_1_dropper(columns)

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

    def __str__(self):
        return f'{self.name} table of {self.__class__}'
