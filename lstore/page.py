from lstore.mempage import MemPage


class Page:
    def __init__(self, n_cols):
        self.data = [MemPage() for _ in range(n_cols)]
        self.N_COLS = n_cols

    def __setitem__(self, key, value):
        """ Overload [] operator for assignment.
        Note: does not check for invalid inputs for @value, i.e., neg vals can
        break DB.
        Arguments:
            - key: int; 0 <= key < MAX_RECORDS
                Write @value to int@key.
            - value: list
                Value to change to.
        Raise:
            IndexError: if @key not in range
        """
        for i, val in enumerate(value):
            if val is not None:
                self.data[i][key] = val

    def __getitem__(self, key):
        """ Overload [] operator for reading. Read value at given @key.
        Arguments:
            - key: tup
                Ex: p[0, [0, 0, 0, 1, 1]] returns values of the last two
                  columns at row 0.
                key[0]: row id of the values to read
                key[1]: binary list that indicates the columns to read.
        Returns:
            List of data for the requested columns at given row.
            None is used for columns that aren't requested.
            Ex: [None, None, None, 92333, 5132]
        Raise:
            IndexError: if @key not in range
        """
        row, bin_query = key
        return [self.data[col][row] if q else None
                for col, q in enumerate(bin_query)]

