from lstore.config import *

class Page:
    def __init__(self):
        """
        All data are assumed to be 64-bit (8-byte) integers.
        A page has a size of 4096 bytes, thus, it can hold 512 ints which can be
        visualized as a flatten 512x8 matrix where each row represents an int:

        int0:   byte0 byte1 byte2 byte3 byte4 byte5 byte6 byte7
        int1:   byte0 byte1 byte2 byte3 byte4 byte5 byte6 byte7
                ...
        int255: byte0 byte1 byte2 byte3 byte4 byte5 byte6 byte7
        """
        # TODO: move these to config file
        self.SIZE_PAGE = 4096    # size of a page in bytes
        self.SIZE_INT = 8        # size of an int in bytes accepted
        self.MAX_RECORDS = 512   # Maximum number of records per page

        self.num_records = 0
        self.data = bytearray(self.SIZE_PAGE)

    def has_capacity(self):
        """
        Returns:
            True if there's space in the page
        """
        return self.num_records < self.MAX_RECORDS

    def write(self, value):
        """ Write value to the next availale position

        Returns:
            True upon success; False if page is full
        """
        if not self.has_capacity():
            return False

        start_idx = self.num_records * 8
        end_idx = start_idx + 8
        self.data[start_idx:end_idx] = value.to_bytes(self.SIZE_INT, 'big')
        self.num_records += 1
        return True

    def read(self, idx):
        """ Read value at given index.
        Arguments:
            - idx: int
                Starting index of the value to be read
        Returns:
            Integer value with the indices of @idx:@idx+8
        """
        return int.from_bytes(self.data[idx:idx+8], 'big')

    def update(self, value, idx):
        """ Change a value to @value for a given starting index @idx.
            This method does not check if the modification violates how l-store
            operates. USE WITH CAUTION. Invalid modification can break the DB.

        Arguments:
            - value: int
                Value to change to.
            - idx: int
                Starting index of the record
        """
        self.data[idx:idx+8] = value.to_bytes(self.SIZE_INT, 'big')

    def index(self, value, first_only=True):
        """ Find the starting index of records that match the given value in
            this page
        Arguments:
            value: int
                Value to look for.
            first_only: bool, default True
                Whether to only look for the first occurance.
        Returns:
            A list of starting indices of matched record
        """
        # TODO: need to revisit this part after implementing deletions where
        #    we mark rows as deleted by giving them a specific value
        value = value.to_bytes(self.SIZE_INT, 'big')
        result = []

        for i in range(self.num_records):
            a = 8 * i
            b = a + 8
            if self.data[a:b] == value:
                result.append(a)
                if first_only:
                    break

        return result
