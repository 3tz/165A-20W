from lstore.page import *

class Partition:
    def __init__(self, n_cols, key_column):
        """
        Partition holds the following attributes:
        base_page: 1-d list of Page obj
            List holds a Page obj for each column where each holds 512 records
            Ex: if @n_cols = 5, then

        tail_pages: 2-d list of Page obj
            Append only Tail pages that associate with the base_page attribute,
                where each row holds 512 tail records for each column. If more
                than 512 modifications are made, a new list of Page obj is
                created and appended to tail_pages.
        Ex: if @n_cols = 5 and 600 modifications are made, then
            base_page:
                [Page obj, Page obj, Page obj, Page obj, Page obj]
            tail_pages:
                [[Page obj, Page obj, Page obj, Page obj, Page obj],
                 [Page obj, Page obj, Page obj, Page obj, Page obj]]
                where the 1st row holds the first 512 modifications and the
                2nd row holds the remaining 88.

        Arguments:
            - n_cols: int
                Number of columns INCLUDING meta-columns & user columns
            - key_column: int
                Index of the column that has the keys.
        """
        self.N_BASE_REC = 0     # Number of base records
        self.N_TAIL_REC = 0     # Numer of tail records
        self.MAX_RECORDS = 512  # Maximum number of records
        self.KEY_COLUMN = key_column
        self.N_COLS = n_cols
        self.base_page = self.__create_new_page()
        self.tail_pages = [self.__create_new_page()]

    def has_capacity(self):
        """
        Returns:
            True if there's space in self.base_page
        """
        return self.N_BASE_REC < self.MAX_RECORDS

    def write(self, *columns):
        """ Write @columns to the next availale position in self.base_page

        Returns:
            True upon success; False if self.base_page in this partition is full
        """
        if not self.has_capacity():
            return False

        for i, col in enumerate(columns):
            if col:
                self.base_page[i].write(col, self.N_BASE_REC*8)

        self.N_BASE_REC += 1
        return True

    def read(self, idx, query_columns):
        """ Read the data at the index for the query_columns
        Arguments:
            - idx: int
                starting index of the record to be read
            - query_columns: list
                List of boolean values INCLUDING THE META-COLS for the columns
                to return.
        """
        # Convert @query_columns to the actual column indices
        cols = [i for i in range(self.N_COLS) if query_columns[i]]

        result = []
        for col in cols:
            result.append(self.base_page[col].read(idx))

        return result

    def index(self, key, first_only=True):
        """
        Returns:
            A list of tuples containing the information below of matched records
            [(page_idx, RID), (page_idx, RID), ...]
              - page_idx: where the record is in the base page
              - RID: the RID of the record.
        """
        result = []
        idxs = self.base_page[self.KEY_COLUMN].index(
            key, self.N_BASE_REC, first_only
        )

        for idx in idxs:
            rid = self.base_page[self.KEY_COLUMN].read(idx)
            result.append((idx, rid))
            if first_only:
                return result
        return result

    def add_new_tail_page(self, n=1):
        """ Add a new page to self.tail_pages
        Arguments:
            n: int, default 1
                Number of pages to add to the tail
        """
        self.tail_pages += [self.__create_new_page() for _ in range(n)]

    def __create_new_page(self):
        """ Internal Method for creating a new blank page
        """
        return [Page() for _ in range(self.N_COLS)]
