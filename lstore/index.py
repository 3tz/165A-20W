from BTrees.IOBTree import IOBTree
from itertools import chain
from lstore.config import Config


class Index:
    """
    Index RID in DB with their values as the keys in BTree.

    Minimal error checking is involved. Operations that are not possible from
      DB, such as performing insertion operation on the same RID, will lead to
      unexpected behaviors.
    """
    def __init__(self, table):
        self.table = table
        # One index for each table. All are empty initially.
        self.I = [None] * table.num_columns
        # number of records for each column
        self.counts = [0] * table.num_columns
        # create indexing for the key column upon initialization
        self.create_index(table.COL_KEY - Config.N_META_COLS)
        # list of columns that have been initialized indexing but still need to
        #   read from the DB
        self.to_be_indexed = []

    def insert(self, column, value, rid):
        """ Insert @rid with key @value.
        Arguments:
            - column: int
                Column index (aka which column) to perform the operation on.
            - value: int
                A value that's in the database
            - rid: int
                RID of the value in the database.
        """
        self.__index_from_db()

        if self.I[column] is not None:
            self.counts[column] += 1
            try:
                # assume value exists
                self.I[column][value].append(rid)
            except KeyError:
                # value doesn't yet exist, so create one
                self.I[column][value] = [rid]
        else:
            raise KeyError

    def delete(self, column, value, rid):
        """ Delete @rid from key @value.
        Arguments:
            - column: int
                Column index (aka which column) to perform the operation on.
            - value: int
                A value removed from the DB
            - rid: int
                RID of the value in the database.
        """
        self.__index_from_db()
        if self.I[column]:
            self.I[column][value].remove(rid)

    def update(self, column, old_value, new_value, rid):
        """ Remove @rid from key @old_value, and insert @rid to key @new_value
        Arguments:
        - column: int
            Column index (aka which column) to perform the operation on.
        - old_value: int
            The old value from the DB
        - old_value: int
            The new value updated in the DB
        - rid: int
            RID of the value in the database.
        """
        self.delete(column, old_value, rid)
        self.insert(column, new_value, rid)

    def locate(self, column, value):
        """ Locate the RIDs of the records that match @value in @column
        Arguments:
            - column: int
                Column index (aka which column) to perform the operation on.
            - value: int
                Value to look for.
        Returns:
            List of RIDs of all records that match @value
        """
        self.__index_from_db()
        try:
            return self.I[column][value]
        except KeyError:
            return []

    def locate_range(self, column, begin, end):
        """ Locate the RIDs of the records that have values between @begin and
            @end in column @column.
        Arguments:
            - column: int
                Column index (aka which column) to perform the operation on.
            - begin: int
                Starting range of the value to look for (inclusive)
            - end: int
                Ending range of the value to look for (exclusive)
        Returns:
            List of RIDs of all records with values in column @column between
                @begin and @end.
        """
        return list(
            chain.from_iterable(
                self.I[column].values(begin, end, excludemax=True)
            )
        )

    def create_index(self, column):
        """ Create index on column @column
        """
        if self.I[column] is None:
            self.I[column] = IOBTree()
            # Queue the column to be indexed
            if max(self.counts) != 0 and column not in self.to_be_indexed:
                self.to_be_indexed.append(column)

    def drop_index(self, column):
        """ Delete index on column @column
        """
        self.I[column] = None

    def indexed_eh(self, column):
        """ whether @column is indexed
        """
        return self.I[column] is not None

    def __index_from_db(self):
        """ add data from db to indexing
        """
        # need to read from DB for those columns that just initialized indexing
        if len(self.to_be_indexed) > 0:
            self.to_be_indexed.sort()
            query_cols = [0] * Config.N_META_COLS
            query_cols += [
                1 if i in self.to_be_indexed else 0
                for i in range(self.table.num_columns)
            ]
            for rid in range(1, max(self.counts)+1):
                for i, val in enumerate(self.table[rid, query_cols]):
                    column = self.to_be_indexed[i]
                    self.counts[column] += 1
                    try:
                        # assume value exists
                        self.I[column][val].append(rid)
                    except KeyError:
                        # value doesn't yet exist, so create one
                        self.I[column][val] = [rid]

        self.to_be_indexed = []
