from lstore.config import *


class MemPage:
    def __init__(self):
        """
        All data are assumed to be 64-bit (8-byte) integers.
        A page has a size of 4096 bytes, thus, it can hold 512 ints which can be
        visualized as a flatten 512x8 matrix where each row represents an int:

        int0:   byte0 byte1 byte2 byte3 byte4 byte5 byte6 byte7
        int1:   byte0 byte1 byte2 byte3 byte4 byte5 byte6 byte7
                ...
        int512: byte0 byte1 byte2 byte3 byte4 byte5 byte6 byte7
        """
        self.data = bytearray(Config.SIZE_PAGE)

    def __setitem__(self, key, value):
        """ Overload [] operator for assignment.
            This method is only an abstraction of low level operation, and it
            DOES NOT check if the modification violates how l-store operates.
            USE WITH CAUTION. Invalid modification can break the DB.
        Arguments:
            - key: int; 0 <= key < MAX_RECORDS
                Write @value to int@key.
            - value: int
                Value to change to.
        Raise:
            IndexError: if @key not in range
        """
        if 0 <= key < Config.MAX_RECORDS:
            self.data[key * 8:(key + 1) * 8] = value.to_bytes(
                Config.SIZE_INT, 'big')
        else:
            raise IndexError

    def __getitem__(self, key):
        """ Overload [] operator for reading. Read value at given @key.
        Arguments:
            - key: int
                Value of int@key to be returned
        Returns:
            Integer value of int@key.

        Raise:
            IndexError: if @key not in range
        """
        if 0 <= key < Config.MAX_RECORDS:
            return int.from_bytes(self.data[key * 8:(key + 1) * 8], 'big')
        else:
            raise IndexError

    def write(self, value, idx):
        """ Write @value to a location with a starting index @idx.
            This method is only an abstraction of low level operation, and it
            DOES NOT check if the modification violates how l-store operates.
            USE WITH CAUTION. Invalid modification can break the DB.
        Arguments:
            - value: int
                Value to change to.
            - idx: int
                Starting index of the record
        """
        self.data[idx:idx + 8] = value.to_bytes(Config.SIZE_INT, 'big')

    def read(self, idx):
        """ Read value at given index.
        Arguments:
            - idx: int
                Starting index of the value to be read
        Returns:
            Integer value with the indices of @idx:@idx+8
        """
        return int.from_bytes(self.data[idx:idx + 8], 'big')
