from lstore.page import *
from lstore.partition import *
from time import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def getcolumnvalue(self,index):
        return self.columns[index]

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
        self.partitions = [
            Partition(
                n_cols=num_columns+self.N_META_COLS,
                key_column=self.KEY_COLUMN)
        ]
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

        data = [None, self.num_records+1, int(time()), None] # meta columns
        data += columns   # user columns
        p = self.partitions[-1] # current partition
        success = p.write(*data)
        # Current Partition.base_page is full
        if not success:
            self.add_new_partition().write(*data)
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
        # TODO: for this MS, only find the first occurance

        # Convert @query_columns to the actual column indices
        cols = [0] * self.N_META_COLS + query_columns

        result = []
        matches = self.index(key, first_only=True)
        # match: (partition_idx, [(page_idx, RID), ...])
        for match in matches:
            p = self.partitions[match[0]]
            # tup: (page_idx, RID)
            for tup in match[1]:
                columns = p.read(tup[0], cols)
                result.append(Record(tup[1], key, columns))

        return result

    def update(self, key, *columns):
        """ Update records with the specified key.

        Arguments:
            - key: int
                Key of the records to look for.
            - columns: list
                List of values to update the column with. If element is None for
                a column, no update will be made to it.
                Ex: [None, None, None, 1]
        """
        # pairs of record info that are matched
        #     0                      1
        # (partition_idx, [(page_idx, RID), (page_idx, RID), ...])
        matches = self.index(key, first_only=True)

        for partition_idx, tups in matches:
            p = self.partitions[partition_idx]
            # tup: (page_idx, RID)
            for tup in tups:
                p.update(key, *columns)


    def index(self, key, first_only=True):
        """ Find partition & page index & RIDs of records whose key matches @key
        Arguments:
            value: int
                Value to look for.
            first_only: bool, default True
                Whether to only look for the first occurance.
        Returns:
            A list containing the information below of the matched record:
                [(partition_idx,
                    [(page_idx, RID),
                     (page_idx, RID), ...]),
                 (partition_idx,
                    [(page_idx, RID),
                     (page_idx, RID), ...]),
                  ... ]
              - partition_idx: where the partition is in self.partitions
              - page_idx: where the record is in base page for that partition
              - RID: the RID of the record.
        """
        result = []

        for i, p in enumerate(self.partitions):
            tups = p.index(key, first_only)
            # Item found in this partition
            if len(tups) > 0:
                result.append((i, tups))
                if first_only:
                    return result
        return result

    def add_new_partition(self):
        """ Add a new partition to self.partitions
        Return:
            Reference to the newly created partition object
        """
        # n_cols = self.num_columns+self.N_META_COLS
        # self.partitions += [Partition(n_cols) for _ in range(n)]
        self.partitions.append(
            Partition(self.num_columns+self.N_META_COLS, self.KEY_COLUMN)
        )
        return self.partitions[-1]

    def __merge_partition(self, rid):
        partitionRid = rid
        partitionId = 0
        while(partitionRid > 512):
            partitionRid -= 512
            partitionId += 1
        
        new_base_page = self.partitions[partitionId].based_page()
        #find the latest tail record
        tail_size = len(self.partitions[partitionId].tail_pages[0])
        ##num_set_tail = len(self.partitions[partitionId].tail_pages) / self.partitions[partitionId].N_COLS
        get_enc = [0,0,1,0].append([0]* (self.partitions[partitionId].N_COLS - self.partitions[partitionId].N_META_COLS))
        new_col = [0]*self.partitions[partitionId].N_COLS
        while tail_size >= 0:
            tb[rid, get_enc]
            tail_size = tail_size -1



        pass

    def __merge(self):
        #when to perform merge
        #iterate from the latest tail page
        
        self.partitions
        pass

    def sum(self, start_range, end_range, aggregate_column_index):
        """ Aggregates column data within the key range given.

        Arguments:
            - start_range: int
                Beginning key.
            - end_range: int
                Ending key
            - aggregate_column_index: int
                Index of the column to be aggregated
        """
        #pass
        query_columns = [0] * self.num_columns
        query_columns[aggregate_column_index] = 1
        total = 0
        for keyval in range(start_range, end_range+1):
            results = self.select(keyval,query_columns)
            if len(results) == 0 :
                continue

            result = results[0]
            total += result.columns[0]
        return total
