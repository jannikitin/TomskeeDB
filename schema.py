"""
Provide instruments for linking different tables of TomskeeDB.

"""

from table import Table


class Schema:

    def __init__(self):
        self.schema = {}

    def create_table(self, table: Table, name=None):
        if name:
            table.name = name
        else:
            name = table.name
        self.schema[name] = table

    def __getitem__(self, key):
        return self.schema[key]


