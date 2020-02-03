from template.page import *
from template.partition import *
from time import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    def __init__(self, name, num_columns, key):
        """
        Table consists of 4 meta-columns (indirection, RID, Timestamp, &
        schema encoding) and user-defined columns.

        Indirection: 0 represents NULL
        RID:         Starts from 1
        Timestamp:
        Schema Encoding:
        Arguments:
            - table: str
                Table name
            - num_columns: int
                Number of Columns excluding meta-columns
            - key: int
                Index of table key in columns
        """
        # CONSTANTS
        self.INDIRECTION_COLUMN = 0
        self.RID_COLUMN = 1
        self.TIMESTAMP_COLUMN = 2
        self.SCHEMA_ENCODING_COLUMN = 3
        self.N_META_COLS = 4
        self.name = name
        self.key = key
        self.num_columns = num_columns

        self.num_records = 0 # keeps track of # of records & RID
        self.partitions = [Partition(n_cols=num_columns+self.N_META_COLS)]
        # TODO: implement page_dir; currently it's just 512 recs / page
        self.page_directory = {}

    def insert(self, *columns):
        """ Write the meta-columns & @columns to the correct page
        Arguments:
            - columns: list
                Record to be written to the DB.
        """
        # Indirection is NULL which we use 0 to represent.
        # Schema is in binary representation which has a default of 0000 or 0 in
        #   base-10.
        # Thus, there's no need to write anything for these two meta-cols since
        #    they are already zeros by default in the page.
        # TODO: currently because of this^, these two cols .num_records are 0

        p = self.partitions[-1] # current partition
        # RID starts from 1
        success = p.base_page[self.RID_COLUMN].write(self.num_records+1)
        # Current Partition.base_page is full
        if not success:
            self.add_new_partition()
            p = self.partitions[-1] # current partition
            p.base_page[self.RID_COLUMN].write(self.num_records+1)

        # UNIX timestamp in seconds
        p.base_page[self.TIMESTAMP_COLUMN].write(int(time()))
        # Write the user columns
        for i in range(len(columns)):
            p.base_page[i+self.N_META_COLS].write(columns[i])
        self.num_records += 1

    def add_new_partition(self, n=1):
        """ Add a new partition to self.partitions
        Arguments:
            n: int, default 1
                Number of pages to add to the tail
        """
        n_cols = self.num_columns+self.N_META_COLS
        self.partitions += [Partition(n_cols) for _ in range(n)]

    def __merge(self):
        pass
