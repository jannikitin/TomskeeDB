"""
Provide instruments for linking different tables of TomskeeDB.

"""

from table import Table
from exceptions import TskException

class Schema:

    def __init__(self):
        self.schema = {}

    def create_table(self, table: Table, name=None):
        table.name = name if name else table.name
        self.schema[name] = table

    def execute(self, query):
        try:
            table = self.schema[query['from'].name]
        except KeyError:
            raise TskException
        columns = query['select']._parameters
        limit = query.get_value('limit')
        offset = query.get_value('offset')
        table.select(columns, limit=limit, offset=offset)

    def __getitem__(self, key):
        return self.schema[key]


