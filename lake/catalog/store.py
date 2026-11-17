import sqlite3
from contextlib import contextmanager

from lake.catalog.schema import initialize_schema
from lake.objects.paths import ensure_storage_root, resolve_storage_paths


class Catalog:
    def __init__(self, root):
        self.paths = resolve_storage_paths(root)
        ensure_storage_root(self.paths)
        self.connection = sqlite3.connect(
            str(self.paths.database), isolation_level=None, timeout=5.0
        )
        try:
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.connection.execute("PRAGMA journal_mode=WAL")
            initialize_schema(self.connection)
        except BaseException:
            self.connection.close()
            raise

    def close(self):
        self.connection.close()

    @contextmanager
    def _transaction(self, statement):
        if self.connection.in_transaction:
            raise RuntimeError("nested catalog transaction")
        self.connection.execute(statement)
        try:
            yield self.connection
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def read_transaction(self):
        return self._transaction("BEGIN")

    def write_transaction(self):
        return self._transaction("BEGIN IMMEDIATE")
