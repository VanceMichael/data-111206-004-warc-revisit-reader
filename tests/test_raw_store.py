import tempfile
import unittest
from pathlib import Path
from raw_store import RawStore


class RawStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / 'raw.sqlite3'
        self.store = RawStore(self.path)

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def test_bytes_round_trip(self):
        body = '资料\x00正文'.encode()
        digest = self.store.save('local', body, {'格式': '原文'}, '2026-11-01T00:00:00Z')
        self.assertEqual(self.store.read(digest), body)

    def test_repeated_observation(self):
        self.store.save('source', b'one', {}, '2026-11-01')
        self.store.save('source', b'one', {}, '2026-11-02')
        self.assertEqual(len(self.store.observations()), 1)
        self.assertEqual(self.store.observations()[0]['last_seen'], '2026-11-02')

    def test_source_isolation(self):
        self.store.save('one', b'same', {}, '2026-11-01')
        self.store.save('two', b'same', {}, '2026-11-01')
        self.assertEqual(len(self.store.observations()), 2)
        self.assertEqual(self.store.connection.execute('SELECT COUNT(*) FROM objects').fetchone()[0], 1)

    def test_reopen(self):
        digest = self.store.save('source', b'persist', {}, '2026-11-01')
        self.store.close()
        self.store = RawStore(self.path)
        self.assertEqual(self.store.read(digest), b'persist')

    def test_invalid_metadata_is_not_partially_saved(self):
        with self.assertRaises(ValueError):
            self.store.save('source', b'bad', {'number': float('nan')}, '2026-11-01')
        self.assertEqual(self.store.observations(), [])

    def test_missing_body(self):
        with self.assertRaises(KeyError):
            self.store.read('missing')

    def test_invalid_source(self):
        with self.assertRaises(ValueError):
            self.store.save('', b'body', {}, '2026-11-01')
