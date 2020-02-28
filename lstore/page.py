from lstore.mempage import MemPage


class Page:
    """ Sample API:
    base_page = Page(5)
    base_page[0] = [1,2,3,4,5]      # write 5 columns to row 0
    base_page[0] = [None]*4 + [10]  # update last column to a value of 10
    base_page[0, 4]                 # read single value at row 0 column 4
    base_page[0, [1,1,0,0,1]]        # read 1+ values at row 0 column 0, 1, 4
    """

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
                Allows multiple or single query.
                Multi query:
                    - key[0]: int
                        row id of the values to read
                    - key[1]: list
                        binary list that indicates the columns to read.
                    Ex: p[0, [0, 0, 0, 1, 1]] returns values of the last two
                      columns at row 0.
                Single query:
                    - key[0]: int
                        row id of the values to read
                    - key[1]: int
                        index of the column to read from.
                    Ex: p[0, 1] returns values of column with index 1 at row 0
        Returns:
            List of data for the requested columns at given row.
            None is used for columns that aren't requested.
            Ex: [None, None, None, 92333, 5132]
        Raise:
            IndexError: if @key not in range
        """
        row, idx = key

        if type(idx) is list:
            # idx is a binary query list, so go through and return a list
            return [self.data[col][row] if q else None
                    for col, q in enumerate(idx)]
        else:
            # idx is just a single integer indicating which column to retrieve
            return self.data[idx][row]

    def __len__(self):
        """ Number of columns in the page
        """
        return len(self.data)