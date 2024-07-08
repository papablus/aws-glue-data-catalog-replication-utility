class DBReplicationStatus:

    def __init__(self):
        self.db_name = None
        self.created = False
        self.error = False

    @property
    def db_name(self):
        return self._db_name

    @db_name.setter
    def db_name(self, db_name):
        self._db_name = db_name

    @property
    def created(self):
        return self._created

    @created.setter
    def created(self, created):
        self._created = created

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, error):
        self._error = error