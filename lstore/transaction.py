class Transaction:
    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.table = None

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still
    # return True if transaction commits or False on abort
    def run(self):
        # do a pre-check of availability of the locks by trying to lock
        # We can assume a transaction will access only one table for this MS
        self.table = self.queries[0][0].__self__.table
        # pre-check failure; locks released and return false
        if not self.table.check_n_lock(self.queries):
            return self.__abort()
        # pre-check success; commit queries
        return self.__commit()

    def __abort(self):
        return False

    def __commit(self):

        for query, args in self.queries:
            query(*args)
        # Also release the locks
        self.table.release_lock(self.queries)
        return True

