from template.page import *

class Partition:
    def __init__(self, n_cols):
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
        """
        self.n_cols = n_cols
        self.base_page = self.__create_new_page()
        self.tail_pages = [self.__create_new_page()]

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
        return [Page() for _ in range(self.n_cols)]
