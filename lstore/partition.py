from lstore.page import Page
from time import time
from lstore.config import Config


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
        self.N_COLS = n_cols
        self.COL_KEY = key_column

        self.count_base_rec = 0     # Number of base records
        self.count_tail_rec = 0     # Number of tail records
        self.__dirty = True         # Whether there has been a modification

        self.base_page = Page(n_cols)
        self.tail_pages = [Page(n_cols)]

        # list of records that have been updated in the base page
        self.updated_idxs = set()

    def has_capacity(self):
        """
        Returns:
            True if there's space in self.base_page
        """
        return self.count_base_rec < Config.MAX_RECORDS

    def write(self, *columns):
        """ Write @columns to the next availale position in self.base_page

        Returns:
            True upon success; False if self.base_page in this partition is full
        """
        if not self.has_capacity():
            return False

        self.base_page[self.count_base_rec] = columns
        self.count_base_rec += 1
        self.__dirty = True
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
        tid = self.base_page[idx, Config.COL_IDR]
        result = []
        # There's an indirection aka tid != 0
        if tid:
            which_tp, where_in_tp = self.__get_tail_page_idx(tid)
            tp = self.tail_pages[which_tp]
            enc = tp[where_in_tp, Config.COL_ENC]
            enc = [int(i) for i in list(format(enc, '0%db' % self.N_COLS))]

            for i, query_this_column in enumerate(query_columns):
                if query_this_column:
                    # If there has been an update, take it from TP
                    if enc[i]:
                        result.append(tp[where_in_tp, i])
                    # if not, take it from BP
                    else:
                        result.append(self.base_page[idx, i])
        # No indirection: just read the base page
        else:
            for i, query_this_column in enumerate(query_columns):
                if query_this_column:
                    result.append(self.base_page[idx, i])

        return result

    def update(self, idx, rid, *columns):
        """ Update records with the specified key.

        Arguments:
            - key: int
                Key of the records to look for.
            - columns: list
                List of values to update the column with. If element is None for
                a column, no update will be made to it.
                Ex: [None, None, None, 14]
        """
        self.updated_idxs.add(idx)
        self.__dirty = True
        # Get encoding in base-10
        # Also, notice that encoding only covers userdefined columns
        enc_bin_list = [0 if col is None else 1 for col in columns]
        enc = sum(x << i for i, x in enumerate(reversed(enc_bin_list)))

        tid = self.base_page[idx, Config.COL_IDR]
        # add a new one if there's not enough space in self.tail_pages
        if len(self.tail_pages)*Config.MAX_RECORDS <= self.count_tail_rec:
            self.tail_pages.append(Page(self.N_COLS))

        # if there's an indirection; aka tid isn't 0
        if tid:
            ts = int(time())
            new_tid = self.count_tail_rec + 1
            old_enc = self.base_page[idx, Config.COL_ENC]
            new_enc = enc | old_enc
            # Base Page:
            #   IDR        RID    TS     ENC      *usercolumns
            #   new_tid    None   None   new_enc   None
            cols = [new_tid, None, None, new_enc]
            cols += [None] * len(columns)
            self.base_page[idx] = cols

            # Tail Page:
            # IDR in tail page that points to base page has a first bit of
            # 1, so add 2**63 to rid
            #   IDR    RID        TS     ENC   *usercolumns
            #   tid    new_tid    ts     enc   columns
            which_tp, where_in_tp = self.__get_tail_page_idx(tid)
            tp = self.tail_pages[which_tp]
            cols_to_write = [tid, new_tid, ts, new_enc]

            # iterate through the new columns
            for i, col in enumerate(columns):
                if col is None:
                    # import ipdb; ipdb.set_trace()
                    # write the old change to the new tail page
                    col = tp[where_in_tp, i + Config.N_META_COLS]
                cols_to_write.append(col)

            which_tp, where_in_tp = self.__get_tail_page_idx(new_tid)
            self.tail_pages[which_tp][where_in_tp] = cols_to_write
        # no indirection
        else:
            # intiialize tid as the tid of the latest slot in tail page
            tid = self.count_tail_rec + 1
            ts = int(time())
            # Base Page:
            #   IDR    RID    TS     ENC   *usercolumns
            #   tid    None   None   enc   None
            cols = [tid, None, None, enc]
            cols += [None] * len(columns)
            self.base_page[idx] = cols

            # Tail Page:
            # IDR in tail page that points to base page has a first bit of
            # 1, so add 2**63 to rid
            #   IDR    RID    TS     ENC   *usercolumns
            #   rid    tid    ts     enc   columns
            which_tp, where_in_tp = self.__get_tail_page_idx(tid)
            # meta_cols for tail_page
            cols = (rid+Config.MARK_1ST_BIT, tid, ts, enc) + columns
            self.tail_pages[which_tp][where_in_tp] = cols

        self.count_tail_rec += 1

    def delete(self, idx):
        self.base_page[idx] = [0] + [None] * (self.N_COLS - 1)
        if idx in self.updated_idxs:
            self.updated_idxs.remove(idx)

    def merge(self):
        """ merge tail pages with base page
        """
        # for every base page index that has been updated
        while len(self.updated_idxs) > 0:
            # base page idx of the record that has been updated
            idx = self.updated_idxs.pop()
            tid = self.base_page[idx, Config.COL_IDR]
            assert(tid != 0)

            which_tp, where_in_tp = self.__get_tail_page_idx(tid)
            tp = self.tail_pages[which_tp]
            enc = tp[where_in_tp, Config.COL_ENC]
            enc = [int(i) for i in list(format(enc, '0%db' % self.N_COLS))]

            # New record merged from the tail pages which will replace the
            # current record in the base page
            # idr, rid, ts, enc
            merged_rec = [0, None, None, 0]
            for i in range(4, self.N_COLS):
                # If there has been an update, take it from TP
                if enc[i]:
                    merged_rec.append(tp[where_in_tp, i])
                # if not, stay the same as in the base page
                else:
                    merged_rec.append(None)
            self.base_page[idx] = merged_rec
        # clear tail page
        self.count_tail_rec = 0
        self.tail_pages = [Page(self.N_COLS)]

    def is_dirty(self):
        return self.__dirty

    def set_clean(self):
        self.__dirty = False

    def __get_tail_page_idx(self, tid):
        """ Internal Method for info for where to find a record in tail page
            based on @tid.
        Returns:
            which_tail_page, where_in_that_tail_page_in_terms_of_starting_idx
        """
        tid -= 1
        rem = tid % Config.MAX_RECORDS
        return int((tid - rem) / Config.MAX_RECORDS), rem
