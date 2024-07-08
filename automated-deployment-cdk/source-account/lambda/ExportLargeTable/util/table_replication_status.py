class TableReplicationStatus:
    def __init__(self):
        self.db_name = None
        self.table_name = None
        self.replication_day = None
        self.table_schema = None
        self.replication_time = None
        self.created = False
        self.updated = False
        self.replicated = False
        self.export_has_partitions = False
        self.partitions_replicated = False
        self.error = False
        self.db_not_found_error = False
 
    def get_schema(self, schema):
        self.table_schema = schema

    @property
    def db_not_found_error(self):
        return self._db_not_found_error

    @db_not_found_error.setter
    def db_not_found_error(self, db_not_found_error):
        self._db_not_found_error = db_not_found_error

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, error):
        self._error = error

    # @property
    # def table_schema(self):
    #     return self.table_schema

    # @table_schema.setter
    def table_schema(self, table_schema):
        self._table_schema = table_schema

    @property
    def db_name(self):
        return self._db_name

    @db_name.setter
    def db_name(self, db_name):
        self._db_name = db_name

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, table_name):
        self._table_name = table_name

    @property
    def replicated(self):
        return self._replicated

    @replicated.setter
    def replicated(self, replicated):
        self._replicated = replicated

    @property
    def replication_day(self):
        return self._replication_day

    @replication_day.setter
    def replication_day(self, replication_day):
        self._replication_day = replication_day

    @property
    def replication_time(self):
        return self._replication_time

    @replication_time.setter
    def replication_time(self, replication_time):
        self._replication_time = replication_time

    @property
    def created(self):
        return self._created

    @created.setter
    def created(self, created):
        self._created = created

    @property
    def updated(self):
        return self._updated

    @updated.setter
    def updated(self, updated):
        self._updated = updated

    @property
    def export_has_partitions(self):
        return self._export_has_partitions

    @export_has_partitions.setter
    def export_has_partitions(self, export_has_partitions):
        self._export_has_partitions = export_has_partitions

    @property
    def partitions_replicated(self):
        return self._partitions_replicated

    @partitions_replicated.setter
    def partitions_replicated(self, partitions_replicated):
        self._partitions_replicated = partitions_replicated
