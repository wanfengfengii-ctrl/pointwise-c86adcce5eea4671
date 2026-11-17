CATALOG_VERSION = 1
FIXED_BUCKET_COUNT = 2
TARGET_BUCKET_COUNT = 3
CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS catalog_info (
    key TEXT PRIMARY KEY, value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_commit INTEGER NOT NULL CHECK(business_commit >= 0),
    bucket_count INTEGER NOT NULL DEFAULT 2
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
CREATE TABLE IF NOT EXISTS layout_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    bucket_count INTEGER NOT NULL CHECK(bucket_count IN (2, 3)),
    phase TEXT NOT NULL
        CHECK(phase IN ('idle', 'copying', 'ready', 'finished')),
    task_id TEXT,
    task_request TEXT,
    total INTEGER NOT NULL DEFAULT 0 CHECK(total >= 0),
    processed INTEGER NOT NULL DEFAULT 0 CHECK(processed >= 0),
    cursor_segment INTEGER NOT NULL DEFAULT 0 CHECK(cursor_segment >= 0),
    cursor_offset INTEGER NOT NULL DEFAULT 0 CHECK(cursor_offset >= 0)
);
CREATE TABLE IF NOT EXISTS layout_tasks (
    request_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE,
    source_buckets INTEGER NOT NULL CHECK(source_buckets = 2),
    target_buckets INTEGER NOT NULL CHECK(target_buckets = 3),
    total INTEGER NOT NULL CHECK(total >= 0),
    source_manifest_id INTEGER NOT NULL REFERENCES manifests(id),
    source_segments TEXT NOT NULL
);
"""


def _add_column(connection, table, name, definition):
    existing = {row[1] for row in connection.execute(
        "PRAGMA table_info(" + table + ")"
    )}
    if name not in existing:
        connection.execute(
            "ALTER TABLE " + table + " ADD COLUMN " + name + " " + definition
        )


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
        # A V1 directory may predate the rebucket capability: its manifests have
        # no layout column and its descriptors omit bucket_count. Filling the
        # column and defaulting descriptors to two buckets keeps it usable.
        _add_column(connection, "manifests", "bucket_count",
                    "INTEGER NOT NULL DEFAULT 2")
        _add_column(connection, "layout_state", "cursor_segment",
                    "INTEGER NOT NULL DEFAULT 0")
        _add_column(connection, "layout_state", "cursor_offset",
                    "INTEGER NOT NULL DEFAULT 0")
        head = connection.execute("SELECT manifest_id FROM current_head").fetchone()
        if head is None:
            cursor = connection.execute(
                "INSERT INTO manifests(business_commit, bucket_count) "
                "VALUES (0, ?)", (FIXED_BUCKET_COUNT,)
            )
            connection.execute(
                "INSERT INTO current_head VALUES (1, ?)", (cursor.lastrowid,)
            )
        connection.execute(
            "INSERT OR IGNORE INTO layout_state "
            "(singleton, bucket_count, phase, task_id, task_request, "
            " total, processed, cursor_segment, cursor_offset) "
            "VALUES (1, ?, 'idle', NULL, NULL, 0, 0, 0, 0)",
            (FIXED_BUCKET_COUNT,),
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
