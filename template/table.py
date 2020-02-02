from template.page import *
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
        # TODO: implement page_dir; currently it's just 512 recs / page
        self.page_directory = {}

        # Initialize pages for all columns
        self.base_pages = [None] * (num_columns + self.N_META_COLS)
        self.tail_pages = [None] * (num_columns + self.N_META_COLS)
        for i in range(len(self.base_pages)):
            self.base_pages[i] = Page()
            self.tail_pages[i] = Page()

    def __merge(self):
        pass
