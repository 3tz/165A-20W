from template.config import *

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
