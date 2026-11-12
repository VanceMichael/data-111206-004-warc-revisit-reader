import gzip
import io
import unittest
from pathlib import Path
from warc_source import records


class WarcTests(unittest.TestCase):
    def test_original_offset_and_body(self):
        raw = Path('fixtures/originals.warc').read_bytes()
        rows = list(records(io.BytesIO(raw)))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['offset'], 0)
        self.assertEqual(raw[rows[1]['offset']:][:8], b'WARC/1.1')
        self.assertIn('合成归档'.encode(), rows[0]['body'])

    def test_revisit_has_no_recovered_payload(self):
        rows = list(records(io.BytesIO(Path('fixtures/later.warc').read_bytes())))
        self.assertEqual(rows[0]['headers']['warc-type'], 'revisit')
        self.assertNotIn('合成归档'.encode(), rows[0]['body'])

    def test_gzip_fixture_contains_two_members(self):
        raw = Path('fixtures/members.warc.gz').read_bytes()
        self.assertEqual(raw.count(b'\x1f\x8b\x08'), 2)
        self.assertEqual(len(list(records(io.BytesIO(gzip.decompress(raw))))), 2)

    def test_truncated_rejected(self):
        with self.assertRaises(ValueError):
            list(records(io.BytesIO(Path('fixtures/truncated.warc').read_bytes())))

    def test_unknown_version(self):
        with self.assertRaises(ValueError):
            list(records(io.BytesIO(b'WARC/0.9\r\n')))

    def test_missing_length(self):
        with self.assertRaises(ValueError):
            list(records(io.BytesIO(b'WARC/1.1\r\nWARC-Type: metadata\r\n\r\n')))
