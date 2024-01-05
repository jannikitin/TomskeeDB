class Keyword:

    def __init__(self, item):
        self.name = item

    def __str__(self):
        return f'Keyword <{self.name.upper()}> {repr(Keyword)}'


class Whitespace:

    def __init__(self):
        self.name = ' '

    def __str__(self):
        return f'Whitespace <" "> {repr(Whitespace)}'


class ExecutionList:

    def __init__(self, parameters=''):
        self.name = parameters
        self._parameters = []

    def __str__(self):
        return f'ExecutionList <{self.name}> {repr(ExecutionList)}'

    def append(self, item):
        self.name += item
        if item != ' ':
            self._parameters.append(item.replace(',', ''))

    def _strip(self):
        self.name = self.name.strip()


class Integer:

    def __init__(self, num):
        self.name = num

    def __str__(self):
        return f'Integer <{self.name}> {repr(Integer)}'


class Numeric:

    def __init__(self, num):
        self.name = num

    def __str__(self):
        return f'Numeric <{self.name}> {repr(Numeric)}'
