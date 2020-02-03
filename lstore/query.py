from lstore.table import Table, Record
from lstore.index import Index
import time

class Query:
    def __init__(self, table):
        """ Init Query to perform different queries on the specified table.
        """
        self.table = table

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        pass

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        self.table.insert(*columns)

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        return self.table.select(key, query_columns)

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        # for i in range(np.size(self.table.key)):
        #     if self.table.key[i] == key:
        #         #TODO: find out where to update the Record in table
        #         for j in range(np.size(self.table.num_columns)):
        #             pass
        #             ## = columns[j]
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
