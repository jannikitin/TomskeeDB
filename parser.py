from collections import OrderedDict
import tokens as tk

available_options = {'select', 'from', 'limit', 'offset'}


class Parser:

    def __init__(self, query: str):
        self.raw = query
        self.tokens = self._parse()

    def _parse(self):
        query = self.raw.lower().split(' ')
        tokens = []
        for item in query:
            tokens.append(tokenize(item))
        return tokens

    def __str__(self):
        res = ''
        for token in self.tokens:
            res += (token.__str__())
            res += '\n'
        return res


class Query:

    def __init__(self, query: str):
        self.tokens = Parser(query)
        self.execution_tree = self._create_execution_tree()

    def _create_execution_tree(self):
        pass

    def __str__(self):
        return f'{self.tokens}'


def tokenize(item):
    if item in available_options:
        return tk.Token(item)
    elif item == ' ':
        return tk.Whitespace()
    elif item.isdigit():
        return tk.Number(item)
    else:
        return tk.ExecutionList(item)
