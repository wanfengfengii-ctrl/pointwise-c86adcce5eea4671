CATALOG_VERSION = 1
FIXED_BUCKET_COUNT = 2
CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS catalog_info (
    key TEXT PRIMARY KEY, value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_commit INTEGER NOT NULL CHECK(business_commit >= 0)
);
CREATE TABLE IF NOT EXISTS manifest_segments (
    manifest_id INTEGER NOT NULL REFERENCES manifests(id),
    position INTEGER NOT NULL,
    descriptor TEXT NOT NULL,
    PRIMARY KEY(manifest_id, position)
);
CREATE TABLE IF NOT EXISTS current_head (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    manifest_id INTEGER NOT NULL REFERENCES manifests(id)
);
CREATE TABLE IF NOT EXISTS commits (
    commit_id INTEGER PRIMARY KEY CHECK(commit_id > 0),
    manifest_id INTEGER NOT NULL UNIQUE REFERENCES manifests(id),
    request_id TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS receipts (
    request_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    commit_id INTEGER NOT NULL UNIQUE REFERENCES commits(commit_id),
    manifest_id INTEGER NOT NULL REFERENCES manifests(id),
    operation_count INTEGER NOT NULL CHECK(operation_count > 0)
);
CREATE TABLE IF NOT EXISTS snapshots (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    manifest_id INTEGER NOT NULL REFERENCES manifests(id)
);
"""


def initialize_schema(connection):
    connection.executescript(CATALOG_DDL)
    connection.execute("BEGIN IMMEDIATE")
    try:
        row = connection.execute(
            "SELECT value FROM catalog_info WHERE key='version'"
        ).fetchone()
        if row is not None and row[0] != CATALOG_VERSION:
            raise ValueError("unsupported catalog version")
        connection.execute(
            "INSERT OR IGNORE INTO catalog_info VALUES ('version', ?)",
            (CATALOG_VERSION,),
        )
        head = connection.execute("SELECT manifest_id FROM current_head").fetchone()
        if head is None:
            cursor = connection.execute(
                "INSERT INTO manifests(business_commit) VALUES (0)"
            )
            connection.execute(
                "INSERT INTO current_head VALUES (1, ?)", (cursor.lastrowid,)
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
