from lstore.partition import *
from time import time
from lstore.index import Index


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
        self.num_columns = num_columns  # constant; lower b/c of tester calls
        self.COL_KEY = key + Config.N_META_COLS
        self.N_TOTAL_COLS = num_columns + Config.N_META_COLS
        self.name = name

        self.num_records = 0  # keeps track of # of records & RID
        self.partitions = [
            Partition(
                n_cols=num_columns+Config.N_META_COLS,
                key_column=self.COL_KEY)
        ]
        self.ind = Index(self)

    def insert(self, *columns):
        """ Write the meta-columns & @columns to the correct page
        Arguments:
            - columns: list
                Record to be written to the DB.
        """
        # Indirection is NULL which we use 0 to represent.
        # Schema is in binary representation which has a default of 0000 or 0
        #   in base-10.
        # Thus, there's no need to write anything for these two meta-cols since
        #    they are already zeros by default in the page.

        data = [None, self.num_records+1, int(time()), None] # meta columns
        data += columns   # user columns
        p = self.partitions[-1]  # current partition
        success = p.write(*data)
        # Current Partition.base_page is full
        if not success:
            self.add_new_partition().write(*data)
        self.num_records += 1

        for i, val in enumerate(columns):
            self.ind.insert(i, val, self.num_records)

    # TODO: add column argument
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
        column = self.COL_KEY
        column -= Config.N_META_COLS
        # Convert @query_columns to the actual column indices
        cols = [0] * Config.N_META_COLS + query_columns

        result = []
        rids = self.ind.locate(column, key)

        # matches = self.index(key, first_only=True)
        # match: (partition_idx, [(page_idx, RID), ...])
        # for match in matches:
        for rid in rids:
            # which_p = int(rid / Config.MAX_RECORDS)
            # where_in_p = rid % Config.MAX_RECORDS
            which_p, where_in_p = self.__rid2pos(rid)
            p = self.partitions[which_p]
            result.append(Record(rid, key, p.read(where_in_p, cols)))

            # p = self.partitions[match[0]]
            # # tup: (page_idx, RID)
            # for tup in match[1]:
            #     columns = p.read(tup[0], cols)
            #     result.append(Record(tup[1], key, columns))

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
        # TODO: ADD @column
        # TODO: update indexing for other columns
        indexing_col = self.COL_KEY
        indexing_col -= Config.N_META_COLS
        if columns[indexing_col] is not None:
            key_change = True
        else:
            key_change = False

        # TODO: currently slect is called even when there's no key modification
        cols = [0] * self.num_columns
        cols[indexing_col] = 1
        old_recs = self.select(key, cols)
        rids = self.ind.locate(indexing_col, key)

        assert(len(old_recs) == len(rids))
        for rid, old_rec in zip(rids, old_recs):
            which_p, where_in_p = self.__rid2pos(rid)
            p = self.partitions[which_p]
            p.update(where_in_p, rid, *columns)
            if key_change:
                new_val = columns[indexing_col]
                self.ind.update(indexing_col, old_rec.key, new_val, rid)

    def add_new_partition(self):
        """ Add a new partition to self.partitions
        Return:
            Reference to the newly created partition object
        """
        self.partitions.append(
            Partition(self.num_columns + Config.N_META_COLS, self.COL_KEY)
        )
        return self.partitions[-1]

    def __merge(self):
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
        query_columns = [0] * self.num_columns
        query_columns[aggregate_column_index] = 1
        total = 0
        for keyval in range(start_range, end_range+1):
            results = self.select(keyval, query_columns)
            if len(results) == 0 :
                continue
            result = results[0]
            total += result.columns[0]
        return total

    def __rid2pos(self, rid):
        """ Internal Method for info for where to find a record in base page
            based on @rid.
        Returns:
            which_partition, where_in_partition
        """
        rid -= 1
        rem = rid % Config.MAX_RECORDS
        return int((rid - rem) / Config.MAX_RECORDS), rem
