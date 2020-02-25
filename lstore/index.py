from BTrees.IOBTree import IOBTree
from itertools import chain


class Index:
    """
    Index RID in DB with their values as the keys in BTree.

    Minimal error checking is involved. Operations that are not possible from
      DB, such as performing insertion operation on the same RID, will lead to
      unexpected behaviors.
    """
    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.I = [None] * table.num_columns
        # create indexing for the key column upon initialization
        self.create_index(table.KEY_COLUMN)

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
        if self.I[column] is not None:
            try:
                # assume value exists
                self.I[column][value].append(rid)
            except KeyError:
                # value doesn't yet exist, so create one
                self.I[column][value] = [rid]

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
        self.I[column] = IOBTree()

    def drop_index(self, column):
        """ Delete index on column @column
        """
        self.I[column] = None
