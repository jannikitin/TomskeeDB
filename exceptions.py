class LimitationException(Exception):

    def __init__(self, msg='Limitation'):
        super().__init__(msg)


class TableConsistencyExeption(Exception):

    def __init__(self, msg='Error'):
        super().__init__(msg)


class DtypeError(TableConsistencyExeption):

    def __init__(self, dtype: str):
        super().__init__(f'Usupported dtype {dtype}')


class ValidationError(Exception):

    def __init__(self, msg='Error'):
        super().__init__(msg)


class SQLError(Exception):

    def __init__(self, msg):
        super.__init__(msg)