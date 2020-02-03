from lstore.page import *
from lstore.partition import *
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
                Human language translation: which column has the keys
        """
        # CONSTANTS
        self.INDIRECTION_COLUMN = 0
        self.RID_COLUMN = 1
        self.TIMESTAMP_COLUMN = 2
        self.SCHEMA_ENCODING_COLUMN = 3
        self.N_META_COLS = 4
        self.KEY_COLUMN = key + self.N_META_COLS
        self.name = name
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

    def select(self, key, query_columns):
        """ Read a record whose key matches the specified @key.

        Arguments:
            - key: int
                Key of the records to look for.
            - query_columns: list
                List of boolean values for the columns to return.
        Returns:
            A list of Record objs that match the key.
        """
        # TODO: improve efficiency
        #       Reduce for-loops
        # TODO: look through tail page after implmenting .update()
        # TODO: for this MS, only find the first occurance

        # Convert @query_columns to the actual column indices
        cols = [
            i+self.N_META_COLS for i in range(self.num_columns) \
                if query_columns[i]]

        result = []
        pairs = self.index(key, first_only=True)
        # pair: (partition idx, page idx, RID)
        for pair in pairs:
            p = self.partitions[pair[0]]
            columns = []
            for col in cols:
                columns.append(
                    int.from_bytes(
                        p.base_page[col].data[pair[1]:pair[1]+8], 'big'))
            result.append(Record(pair[2], key, columns))

        return result


    def update(self, key, *columns):
        """ Update records with the specified key.

        Arguments:
            - key: int
                Key of the records to look for.
            - columns: list
                List of values to update the column with. If element is None for
                a column, no update will be made to it.
        """
        for p in self.partitions:
            pass


    def index(self, key, first_only=True):
        """ Find the partition index and RIDs of records whose key matches @key.
        Arguments:
            value: int
                Value to look for.
            first_only: bool, default True
                Whether to only look for the first occurance.
        Returns:
            A tuple containing the information below of the matched record.
            (partition_idx, page_idx, RID)
              - partition_idx: where the partition is in self.partitions
              - page_idx: where the record is in the base page
              - RID: the RID of the record.
        """
        result = []
        for i, p in enumerate(self.partitions):
            idxs = p.base_page[self.KEY_COLUMN].index(key, first_only)
            for idx in idxs:
                rid = int.from_bytes(
                    p.base_page[self.KEY_COLUMN].data[idx:idx+8], 'big',
                )
                result.append((i, idx, rid))
                if first_only:
                    return result
        return result

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
