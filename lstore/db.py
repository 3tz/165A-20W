from lstore.table import Table


class Database():
    def __init__(self):
        self.tables = {}
        pass

    def open(self, path):
        pass

    def close(self):
        pass

    def create_table(self, name, num_columns, key):
        """ Creates a new table
        Arguments:
            - name: str
                Table name
            - num_columns: int
                Number of Columns: all columns are integer
            - key: int
                Index of table key in columns
        Returns:
            Table obj of the table that was added to the DB.
        """
        table = Table(name, num_columns, key)
        self.tables[name] = table
        return table

    def drop_table(self, name):
        """ Deletes the specified table from DB
        Arguments:
            - name: str
                Name of the table to be deleted.
        """
        if name in self.tables.keys():
            del self.tables[name]
