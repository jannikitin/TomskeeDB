import string
from collections import OrderedDict
import tokens as tk
from decimal import Decimal, InvalidOperation
from collections import defaultdict

available_options = {'select', 'from', 'limit', 'offset'}
ORDERING = ['from', 'where', 'group by', 'having', 'select', 'order by', 'offset', 'limit']


def is_decimal(num):
    try:
        Decimal(num)
    except InvalidOperation:
        return False
    return True


def split(sql) -> list:
    sql = sql.strip(' ').lower() + ' '
    splited = []
    prev = 0
    for i, item in enumerate(sql):
        if item in {' '}:
            splited.append(sql[prev:i])
            splited.append(item)
            prev = i + 1
    return splited[0:len(splited) - 1]


def tokenize(item):
    if item in available_options:
        return tk.Keyword(item)
    elif item == ' ':
        return tk.Whitespace()
    elif item.isdigit():
        return tk.Integer(int(item))
    elif is_decimal(item):
        return tk.Numeric(float(item))
    else:
        return tk.ExecutionList(item)


def _parse(query):
    query = split(query)
    tokens = []
    for item in query:
        tokens.append(tokenize(item))
    tokens = Parser._collect_execution_list(tokens)
    return tokens


class Parser:

    def __init__(self, query: str):
        self.tokens = _parse(query)

    @classmethod
    def _collect_execution_list(cls, tokens):
        rewrited = []
        i = 0
        while i < len(tokens):
            if isinstance(tokens[i], tk.ExecutionList):
                lst = tk.ExecutionList()
                while i < len(tokens) and isinstance(tokens[i], (tk.ExecutionList, tk.Whitespace)):
                    lst.append(tokens[i].name)
                    i += 1
                lst._strip()
                rewrited.append(lst)
            else:
                rewrited.append(tokens[i])
                i += 1

        return rewrited

    def __str__(self):
        res = ''
        for token in self.tokens:
            res += (token.__str__())
            res += '\n'
        return res

    def __iter__(self):
        for item in self.tokens:
            yield item


class Query:

    def __init__(self, query: str):
        self._tokens = Parser(query)
        self.execution_tree = self._create_execution_tree()

    def _create_execution_tree(self):
        d = OrderedDict()
        curr = None
        for token in self._tokens:
            if isinstance(token, tk.Keyword):
                curr = token
            if curr:
                if isinstance(token, (tk.ExecutionList, tk.Numeric, tk.Integer)):
                    d[curr.name] = token
        return d

    def get_value(self, key, base=None):
        if key in self.execution_tree.keys():
            return self.execution_tree[key].name
        else:
            return base

    def __repr__(self):
        return self._tokens.__str__()

    def __str__(self):
        s = ''
        for key in self.execution_tree:
            s += f'{str(key)}: '
            s += f'{self.execution_tree[key]} \n'
        return s

    def __getitem__(self, item):
        return self.execution_tree[item]
