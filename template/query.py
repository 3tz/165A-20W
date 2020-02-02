from template.table import Table, Record
from template.index import Index
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
        """ Write the meta-columns & @columns
        Indirection is NULL which we use 0 to represent.
        Schema is in binary representation which has a default of 0000 or 0 in
          base-10.
        Thus, there's no need to write anything for these two meta-cols since
           they are already zeros by default in the page.

        Arguments:
            - columns: list
                Record to be written to the DB.
        """
        # TODO: add new page when page is full
        
        # RID starts from 1
        self.table.base_pages[self.table.RID_COLUMN].write(
            self.table.num_records+1)
        # UNIX timestamp in seconds
        self.table.base_pages[self.table.TIMESTAMP_COLUMN].write(
            int(time.time()))
        # Write the user columns
        for i in range(len(columns)):
            self.table.base_pages[i+self.table.N_META_COLS].write(columns[i])



    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        for i in range(np.size(self.table.key)):
            if self.table.key[i] == key:
                query_columns = self.table.columns[i]
        return query_columns
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
