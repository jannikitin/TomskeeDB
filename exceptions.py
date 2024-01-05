class TskException(Exception):

    def __init__(self, msg='Error'):
        super().__init__(msg)


class ValidationException(Exception):

    def __init__(self, msg='Error'):
        super().__init__(msg)
