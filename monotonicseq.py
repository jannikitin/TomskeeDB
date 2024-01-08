"""
MonotonicSequence -- simplier version of well known Series object from Pandas

"""
from typing import Any, Union, List

import numpy as np

from exceptions import TableConsistencyExeption, ValidationError, DtypeError


class MonotonicSequence:
    """
    One-dimensional special array, e.x. column of Table object of TomskeeDB architecture

    """
    def __init__(self, data: Union[List[Any], np.ndarray], name: str = 'MonotonicSequence', dtype=None):
        try:
            self.data = np.array(data, dtype=dtype)
        except ValueError as e:
            raise DtypeError(f'{e.args[0]}')
        self.name = name
        self.dtype = self.data.dtype

    def append(self, item):
        self.data = np.append(self.data, item)

    def delete(self, index):
        self.data = np.delete(self.data, index)

    def __str__(self):
        return f'{self.name} ({self.data})'

    def __getitem__(self, item):
        return self.data[item]

    def __len__(self):
        return len(self.data)

