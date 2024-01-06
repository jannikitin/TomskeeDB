class TableConsistencyExeption(Exception):

    def __init__(self, msg='Error'):
        super().__init__(msg)


class DtypeException(TableConsistencyExeption):

    def __init__(self, dtype: str):
        super().__init__(f'Usupported dtype {dtype}')


class ValidationException(Exception):

    def __init__(self, msg='Error'):
        super().__init__(msg)


class SQLError(Exception):

    def __init__(self, msg):
        super.__init__(msg)