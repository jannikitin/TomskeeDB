"""
Provide access to data, that's storing in table object of TomskeeDB. Can be linked with other
tables by using PRIMARY KEY. Stores in schema object.

"""
from typing import List, Dict, Union, Any

import numpy as np
from tabulate import tabulate

import tomskeedb as tsk
from exceptions import TableConsistencyExeption, ValidationError, DtypeError
from monotonicseq import MonotonicSequence


class Table:

    def __init__(self, data: Union[np.ndarray, List[Any]] = None,
                 columns: List[str] = None,
                 dtypes: Union[List[type], Dict[str, type]] = None,
                 name: str = 'Table'):
        """


        """
        self.columns = self._create_columns(columns, data)
        self._data = self._create_data(data, dtypes)
        self.name = name if isinstance(name, str) else 'Table'
        self.dtypes = self._define_dtypes()

    @property
    def is_empty(self):
        return len(self._data) == 0

    @property
    def shape(self):
        if self.is_empty:
            return 0, 0
        else:
            return len(self._data[0]), len(self.columns)

    @staticmethod
    def _create_columns(columns, data=None):
        if columns:
            tsk.columns_validation(columns)
            return columns
        else:
            if data:
                return [str(x) for x in range(len(data))]
            else:
                return []

    def _create_data(self, data, dtypes) -> list:
        if data is not None:
            if dtypes is None:
                dtypes = {col: None for col in self.columns}
            table = [0 for _ in range(len(self.columns))]
            for i, seq in enumerate(data):
                col = self.columns[i]
                dt = dtypes[col]
                table[i] = MonotonicSequence(seq, col, dt)
            return table
        else:
            return []

    def _define_dtypes(self):
        if self.is_empty:
            return {}
        dt = {}
        for i, seq in enumerate(self._data):
            col = self.columns[i]
            dt[col] = seq.dtype
        return dt

    def get(self, columns: List[str] = ['*'],
            limit: int = None,
            offset: int = None):
        index_range, columns = self._get_selection_parameters(columns, limit, offset)
        data = []
        for col in columns:
            data.append(self[col][index_range[0]:index_range[1]])
        return Table(data=data, columns=columns)

    def select(self, columns: List[str] = ['*'],
               limit: int = None,
               offset: int = None):
        index_range, columns = self._get_selection_parameters(columns, limit, offset)

        table_data = [[0 for _ in range(len(columns))] for _ in range(index_range[1] - index_range[0])]
        table_idx = 0
        for i in range(index_range[0], index_range[1]):
            for j, col in enumerate(columns):
                table_data[table_idx][j] = self[col][i]
            table_idx += 1
        print(tabulate(table_data, headers=columns, tablefmt="pretty"))

    def _get_selection_parameters(self, columns: List[str] = ['*'],
                                  limit: int = None,
                                  offset: int = None):
        index_range = self._index_ranger(limit, offset)
        if columns == ['*']:
            columns = self.columns
        else:
            for col in columns:
                if col not in self.columns:
                    raise ValidationError(f'{col} not found in axis')
        return index_range, columns

    def _index_ranger(self, limit, offset) -> tuple:
        rng = []
        rng.append(0) if offset is None else rng.append(offset)
        rng.append(self.shape[0]) if limit is None else rng.append(rng[0] + limit)
        if rng[0] < 0 or rng[0] > self.shape[0] or rng[1] > self.shape[0] or rng[1] < 0:
            raise ValidationError(
                f'Index is out of range. Possible range = (0, {self.shape[0]}),'
                f' selected range = ({rng[0]}, {rng[1]})')
        return tuple(rng)

    def insert(self, data,
               axis: int = 0,
               columns: List[str] = None,
               dtypes: List[str] = None):
        if axis == 0:
            self._axis_0_insert(data)
        else:
            columns = self._create_columns(columns, data)
            self._axis_1_insert(data, columns, dtypes)

    def _axis_0_insert(self, data):
        if self.is_empty:
            if len(data) < self.shape[1]:
                data = data[:, np.newaxis]
            self.__init__(data=data)
            return
        for i, col in enumerate(self.columns):
            self[col].append(data[i])

    def _axis_1_insert(self, data, columns, dtypes=None):
        if self.is_empty:
            self.__init__(data=data, columns=columns)
            return
        if dtypes is None:
            dtypes = {col: None for col in columns}
        for i, col in enumerate(columns):
            if len(data[i]) != self.shape[0]:
                raise TableConsistencyExeption(f'Insertion column must be the same size as other columns '
                                               f'{i}-th columns length is {len(data[i])}')
            self.__setitem__(value=data[i], key=col, dtype=dtypes[col])
            self.columns.append(col)
        self._define_dtypes()

    def drop(self, columns: List[str] = None,
             index: List[int] = None,
             axis: int = 0):
        if not index and not columns:
            raise TableConsistencyExeption('Column and index are empty')
        if axis == 0:
            self._axis_0_dropper(index)
        else:
            columns = self._create_columns(columns)
            self._axis_1_dropper(columns)

    def _axis_0_dropper(self, index):
        for i in index:
            for col in self.columns:
                self[col].delete(i)

    def _axis_1_dropper(self, columns):
        for col in columns:
            self.delete(col)
        self.update()

    def update(self):
        self.columns = [x.name for x in self._data]
        self.dtypes = self._define_dtypes()

    def delete(self, col):
        for i, seq in enumerate(self._data):
            if seq.name == col:
                del (self._data[i])

    def to_csv(self, FILE_NAME: str):
        path = f'data//{FILE_NAME}.txt'
        f = open(path, 'w')
        f.write(','.join([x for x in self.columns]) + '\n')
        for i in range(self.shape[0]):
            for j, key in enumerate(self.columns):
                f.write(self._data[key][i])
                if j < len(self.columns) - 1:
                    f.write(',')
                else:
                    f.write('\n')
        f.close()

    def __str__(self):
        if self.is_empty:
            return 'Table is empty'
        res = ''
        for seq in self._data:
            res += seq.__str__() + '\n'
        return res

    def __repr__(self):
        if self.is_empty:
            return 'Table is empty'
        return f'Table name: {self.name}\nData: {self._data[0]}.. \nColumns: {self.columns}\nDtype: {self.dtypes}'

    def __len__(self):
        return self.shape

    def __getitem__(self, item):
        for seq in self._data:
            if seq.name == item:
                return seq
        raise KeyError(f'{item} not found in axis')

    def __setitem__(self, key, value, dtype=None):
        self._data.append(MonotonicSequence(data=value, name=key, dtype=dtype))

