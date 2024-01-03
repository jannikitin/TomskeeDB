class Token:

    def __init__(self, item):
        self.name = item

    def __str__(self):
        return f'{self.__class__} with value {self.name.upper()}'


class Whitespace:

    def __init__(self):
        self.name = 'whitespace'

    def __str__(self):
        return f'{self.__class__} with value {self.name}'


class ExecutionList:

    def __init__(self, parameters):
        self.name = parameters

    def __str__(self):
        return f'{self.__class__} with values {self.name}'

class Number:

    def __init__(self, num):
        self.name = num

    def __str__(self):
        return f'{self.__class__} with value {self.name}'
