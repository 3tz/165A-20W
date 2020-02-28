# Global Setting for the Database
# PageSize, StartRID, etc..


class Config:
    # configurations for base page
    COL_IDR = 0
    COL_RID = 1
    COL_TS = 2
    COL_ENC = 3
    N_META_COLS = 4
    MARK_1ST_BIT = 9223372036854775808  # 2^63
    # config for MemPage
    SIZE_PAGE = 4096  # size of a page in bytes
    SIZE_INT = 8  # size of an int in bytes accepted
    MAX_RECORDS = 512  # Maximum number of records per page
    # bufferpool
    SIZE_BUFFER = 4  # number of partitions in the bufferpool


def init():
    """ To make the tester happy
    """
    pass
