from lstore.page import *
from time import time
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
        self.INDIRECTION_COLUMN = 0 # TODO: initialize them from Table
        self.RID_COLUMN = 1
        self.TIMESTAMP_COLUMN = 2
        self.SCHEMA_ENCODING_COLUMN = 3
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
        self.__write(self.base_page, self.N_BASE_REC*8, *columns)
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
            rid = self.base_page[self.RID_COLUMN].read(idx)
            result.append((idx, rid))
            if first_only:
                return result
        return result

    def update(self, key, *columns):
        """ Update records with the specified key.

        Arguments:
            - key: int
                Key of the records to look for.
            - columns: list
                List of values to update the column with. If element is None for
                a column, no update will be made to it.
                Ex: [None, None, None, 14]
        """
        # Get encoding in base-10
        # Also, notice that encoding only covers userdefined columns
        enc_bin_list = [0 if col == None else 1 for col in columns]
        enc = sum(x << i for i, x in enumerate(reversed(enc_bin_list)))

        tups = self.index(key)
        # (page_idx, RID)
        for idx_base, rid in tups:
            rid = self.base_page[self.RID_COLUMN].read(idx_base)
            tid = self.base_page[self.INDIRECTION_COLUMN].read(idx_base)
            # add a new one if there's not enough space in self.tail_pages
            if len(self.tail_pages) * self.MAX_RECORDS <= self.N_TAIL_REC:
                self.__add_new_tail_page()

            # if there's an indirection; aka tid isn't 0
            if tid:
                pass
            # no indirection
            else:
                # intiialize tid as the tid of the latest slot in tail page
                tid = self.N_TAIL_REC+1
                ts = int(time())
                # Base Page:
                #   IDR    RID    TS     ENC   *usercolumns
                #   tid    None   None   enc   None
                cols = [tid, None, None, enc]
                cols += [None] * len(columns)
                self.__write(self.base_page, idx_base, *cols)

                # Tail Page:
                #   IDR    RID    TS     ENC   *usercolumns
                #   rid    tid    ts     enc   columns
                which_tp, where_in_tp = self.__get_tail_page_idx(tid)
                # meta_cols for tail_page
                cols = (rid, tid, ts, enc) + columns
                self.__write(self.tail_pages[which_tp], where_in_tp, *cols)

            self.N_TAIL_REC += 1

    def __add_new_tail_page(self):
        """ Internal method for adding a new page to self.tail_pages
        """
        self.tail_pages.append(self.__create_new_page())

    def __get_tail_page_idx(self, tid):
        """ Internal Method for info for where to find a record in tail page
            based on @tid.
        Returns:
            which_tail_page, where_in_that_tail_page_in_terms_of_starting_idx
        """
        rem = tid % self.MAX_RECORDS
        return int((tid - rem) / self.MAX_RECORDS), (rem-1) * 8

    def __write(self, single_page, page_idx, *columns):
        """ Internal Method for writing @columns to @nth_rec position in
            @single_page .
        """
        for i, col in enumerate(columns):
            if col:
                single_page[i].write(col, page_idx)

    def __create_new_page(self):
        """ Internal Method for creating a new blank page
        """
        return [Page() for _ in range(self.N_COLS)]
