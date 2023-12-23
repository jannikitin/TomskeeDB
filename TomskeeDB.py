import numpy as np
from collections.abc import Callable
from typing import List, Dict, Any


class InputException(Exception):

    def __init__(self, message='Columns and data in array must be the same size'):
        super().__init__(message)


class Database:
    pass


class Schema:
    pass


class Table:


    # can take list data, dict data and np.array
    def __init__(self, data, columns: list = None, **kwargs):
        self.data = {}
        self.shape = tuple()

        self.create_shape(data, columns)
        self.shape = (len(self.raw_data_), len(self.columns_))
        self.create_data()


    def create_shape(self, data, columns):
        if isinstance(data, dict):
            self.columns_ = np.array([x for x in data.keys()])
            self.raw_data_ = np.array([x for x in data.values()])
            for i, col in enumerate(self.columns_):
                self.data[col] = np.array(self.raw_data_[i])
        elif isinstance(data, list):
            if columns:
                if len(columns) != len(data[0]):
                    raise InputException
                self.raw_data_ = np.array([x for x in data])
                self.columns_ = np.array([x for x in columns])
            else:
                self.columns_ = np.array([col for col in range(0, len(data[0]))])
                self.raw_data_ = np.array([x for x in data])

        elif type(data) == np.ndarray:
           if columns:
                if len(columns) != len(data[0]):
                    raise InputException
                self.raw_data_ = data
                self.columns_ = np.array(columns)
           else:
               self.raw_data_ = data
               self.columns_ = np.array([x for x in range(len(data))])
        else:
            raise InputException('Incorrect type')


    def create_data(self):
        if self.data is True:
            return
        for i, col in enumerate(self.columns_):
            self.data[col] = [x[i] for x in self.raw_data_]

    def get(self, cols, num_of_rows):
        data = []
        for i in range(num_of_rows):
            for key in cols:
                data.append(self.data[key][i])
        print(*(col for col in cols))
        for i in range(0, len(data), len(cols)):
            step = len(cols)
            print(*(x for x in data[i:i + step]))

    def print(self):
        print(self.data)

    @staticmethod
    def valid_input(cols, data):
        return len(cols) == len(data[0])
