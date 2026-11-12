import hashlib
import json
import sqlite3
from pathlib import Path


class RawStore:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=10)
        self.connection.execute('PRAGMA journal_mode=WAL')
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS objects (
                digest TEXT PRIMARY KEY, body BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS observations (
                source TEXT NOT NULL, digest TEXT NOT NULL REFERENCES objects(digest),
                metadata TEXT NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
                PRIMARY KEY(source, digest));
        """)

    def save(self, source, body, metadata, seen_at):
        if not source or not isinstance(body, bytes) or not seen_at:
            raise ValueError('来源、原始字节和观察时间不可缺少')
        encoded = json.dumps(metadata, ensure_ascii=False, sort_keys=True, allow_nan=False)
        digest = hashlib.sha256(body).hexdigest()
        with self.connection:
            self.connection.execute('INSERT OR IGNORE INTO objects VALUES(?,?)', (digest, body))
            self.connection.execute("""
                INSERT INTO observations VALUES(?,?,?,?,?)
                ON CONFLICT(source,digest) DO UPDATE SET
                    metadata=excluded.metadata, last_seen=MAX(last_seen,excluded.last_seen)
            """, (source, digest, encoded, seen_at, seen_at))
        return digest

    def read(self, digest):
        row = self.connection.execute('SELECT body FROM objects WHERE digest=?', (digest,)).fetchone()
        if row is None:
            raise KeyError(digest)
        return bytes(row[0])

    def observations(self):
        rows = self.connection.execute(
            'SELECT source,digest,metadata,first_seen,last_seen FROM observations ORDER BY source,digest')
        return [dict(zip(('source', 'digest', 'metadata', 'first_seen', 'last_seen'), row)) for row in rows]

    def close(self):
        self.connection.close()
