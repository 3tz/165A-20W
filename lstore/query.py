from lstore.table import Table, Record
from lstore.index import Index
import time


class Query:
    """ This is just a wrapper class of Table methods. See table.py for actual
        implementation of each method.
    """
    def __init__(self, table):
        """ Init Query to perform different queries on the specified table.
        """
        self.table = table

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        self.table.delete(key)

    def insert(self, *columns):
        self.table.insert(*columns)

    def select(self, key, indexing_col, query_columns):
        return self.table.select(key, indexing_col, query_columns)

    def update(self, key, *columns):
        self.table.update(key, *columns)

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        return self.table.sum(start_range, end_range, aggregate_column_index)
