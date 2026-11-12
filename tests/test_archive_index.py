"""归档索引端到端测试：解析、补索引、revisit 解析、损坏隔离与失效检测。

全部使用 make_archives.py 生成的本地合成 fixtures，或在临时目录中现场构造，
不访问网络。Python 以 -I 隔离模式运行。
"""

import gzip
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import warc_source as ws
from archive_index import ArchiveIndex, StaleArchiveError

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
ORIGINAL_ID = "urn:uuid:00000000-0000-4000-8000-000000000001"
REVISIT_ID = "urn:uuid:00000000-0000-4000-8000-000000000002"
LOOP_ID = "urn:uuid:00000000-0000-4000-8000-000000000003"
ENCODED_ID = "urn:uuid:00000000-0000-4000-8000-000000000005"
BAD_DIGEST_ID = "urn:uuid:00000000-0000-4000-8000-000000000006"
URL = "https://archive.example/product/1"


def build_record(kind, identity, body, **extra):
    headers = {
        "WARC-Type": kind,
        "WARC-Record-ID": f"<urn:uuid:{identity}>",
        "WARC-Date": extra.pop("warc_date", "2026-11-01T00:00:00Z"),
        "WARC-Target-URI": URL,
        "Content-Type": "application/http; msgtype=response",
        **extra,
        "Content-Length": str(len(body)),
    }
    head = "WARC/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
    return head.encode() + body + b"\r\n\r\n"


class IndexHarness(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        # 不可信归档放在独立空目录，脚本不进入该目录运行。
        self.arc_dir = self.root / "archives"
        self.arc_dir.mkdir()
        for name in ("originals.warc", "later.warc", "encoded.warc",
                     "members.warc.gz", "truncated.warc"):
            shutil.copy2(FIXTURES / name, self.arc_dir / name)
        self.db = self.root / "index.sqlite3"
        self.index = ArchiveIndex(self.db)

    def tearDown(self):
        self.index.close()
        self.tmp.cleanup()

    def p(self, name):
        return str(self.arc_dir / name)


class ParsingTests(IndexHarness):
    def test_indexes_expected_record_types_and_locations(self):
        report = self.index.index_paths([self.p("originals.warc"), self.p("later.warc")])
        self.assertEqual(len(report.indexed), 2)
        rows = self.index.connection.execute(
            "SELECT warc_type,record_offset,content_length,compressed_offset FROM records ORDER BY record_pk"
        ).fetchall()
        self.assertEqual([r["warc_type"] for r in rows],
                         ["response", "metadata", "revisit", "revisit"])
        self.assertIsNone(rows[0]["compressed_offset"])
        self.assertEqual(rows[0]["record_offset"], 0)

    def test_gzip_members_keep_file_offset_and_decompressed_length(self):
        report = self.index.index_paths([self.p("members.warc.gz")])
        self.assertEqual(report.indexed[0][1], 2)
        rows = self.index.connection.execute(
            "SELECT record_offset,compressed_offset,compressed_span,decompressed_length"
            " FROM records ORDER BY record_pk").fetchall()
        self.assertEqual([r["compressed_offset"] for r in rows], [0, 320])
        self.assertEqual([r["decompressed_length"] for r in rows], [419, 526])
        # 索引里的成员跨度必须与真实 gzip 成员边界一致。
        blob = (self.arc_dir / "members.warc.gz").read_bytes()
        members = list(ws.iter_gzip_members(blob))
        self.assertEqual([(m.file_offset, m.compressed_span) for m in members],
                         [(rows[0]["compressed_offset"], rows[0]["compressed_span"]),
                          (rows[1]["compressed_offset"], rows[1]["compressed_span"])])

    def test_random_read_opens_only_the_target_member(self):
        self.index.index_paths([self.p("members.warc.gz")])
        second = self.index.connection.execute(
            "SELECT record_pk FROM records ORDER BY record_pk DESC LIMIT 1").fetchone()
        reads = []

        class RecordingIndex(ArchiveIndex):
            def _open(self, path):
                inner = open(path, "rb")

                class Wrapper:
                    def seek(self, pos, *args):
                        return inner.seek(pos, *args)

                    def read(self, size=-1):
                        start = inner.tell()
                        data = inner.read(size)
                        reads.append((start, len(data)))
                        return data

                    def __enter__(self):
                        return self

                    def __exit__(self, *args):
                        inner.close()

                return Wrapper()

        recording = RecordingIndex(self.db)
        try:
            content = recording.read_record_bytes(record_pk=second["record_pk"])
        finally:
            recording.close()
        self.assertIn(b"HTTP/1.1 200", content)
        # 随机读取只在第二个成员的文件偏移处取压缩片段，从不从 0 读整批归档。
        self.assertTrue(reads)
        self.assertTrue(all(start >= 320 for start, _ in reads))
        total_bytes = sum(size for _, size in reads)
        self.assertLessEqual(total_bytes, 334)  # 仅第二个成员压缩跨度

    def test_chunked_and_gzip_http_is_decoded(self):
        self.index.index_paths([self.p("encoded.warc")])
        result = self.index.get_by_record_id(ENCODED_ID)
        self.assertIsNone(result["http"]["decode_error"])
        self.assertEqual(result["http"]["entity_length"], 44)
        opened = self.index.open_record(result)
        self.assertEqual(opened["payload"].decode(), "合成归档产品甲")

    def test_raw_archive_position_and_capture_time_are_retained(self):
        self.index.index_paths([self.p("originals.warc")])
        result = self.index.get_by_record_id(ORIGINAL_ID)
        self.assertEqual(result["location"]["file"], self.p("originals.warc"))
        self.assertEqual(result["location"]["record_offset"], 0)
        self.assertEqual(result["warc_date"], "2026-11-01T00:00:00Z")


class RevisitResolutionTests(IndexHarness):
    def test_cross_file_revisit_composes_header_and_payload(self):
        # 先索引后到达的 revisit 文件，再索引被引用的原始文件。
        self.index.index_paths([self.p("later.warc")])
        unresolved = self.index.get_by_record_id(REVISIT_ID)
        self.assertFalse(unresolved["resolution"]["resolved"])
        self.assertIn("本地索引中不存在", unresolved["resolution"]["reason"])
        # 补索引后引用自动可解析，无需重写 revisit 行。
        self.index.index_paths([self.p("originals.warc")])
        resolved = self.index.get_by_record_id(REVISIT_ID)
        self.assertTrue(resolved["resolution"]["resolved"])
        self.assertEqual(resolved["resolution"]["header_source"]["record_id"], REVISIT_ID)
        self.assertEqual(resolved["resolution"]["payload_source"]["record_id"], ORIGINAL_ID)
        opened = self.index.open_record(resolved)
        self.assertEqual(opened["http_header_source"]["record_id"], REVISIT_ID)
        self.assertEqual(opened["payload_source"]["record_id"], ORIGINAL_ID)
        self.assertEqual(opened["payload"].decode(), "合成归档产品甲")

    def test_reference_loop_is_reported_not_followed(self):
        self.index.index_paths([self.p("later.warc"), self.p("originals.warc")])
        result = self.index.get_by_record_id(LOOP_ID)
        self.assertFalse(result["resolution"]["resolved"])
        self.assertIn("引用环", result["resolution"]["reason"])

    def test_digest_mismatch_refuses_payload(self):
        self.index.index_paths([self.p("encoded.warc"), self.p("originals.warc")])
        result = self.index.get_by_record_id(BAD_DIGEST_ID)
        self.assertFalse(result["resolution"]["resolved"])
        self.assertIn("载荷摘要不符", result["resolution"]["reason"])
        # open_record 不得给未解析 revisit 拼接外部载荷。
        opened = self.index.open_record(result)
        self.assertEqual(opened["payload_source"]["record_id"], BAD_DIGEST_ID)
        self.assertEqual(opened["payload"], b"")

    def test_language_representation_is_not_substituted(self):
        # 构造一个 zh revisit 指向 en response：语言冲突必须拒绝。
        en_payload = "english product".encode()
        en_record = build_record(
            "response", "aaaaaaaa-0000-4000-8000-0000000000aa",
            b"HTTP/1.1 200 OK\r\nContent-Language: en\r\nContent-Length: "
            + str(len(en_payload)).encode() + b"\r\n\r\n" + en_payload,
            **{"WARC-Payload-Digest": "sha1:ENGLISHDIGESTDUMMYVALUE000000000"})
        zh_revisit = build_record(
            "revisit", "bbbbbbbb-0000-4000-8000-0000000000bb",
            b"HTTP/1.1 200 OK\r\nContent-Language: zh\r\n\r\n",
            warc_date="2026-11-03T00:00:00Z",
            **{"WARC-Refers-To": "<urn:uuid:aaaaaaaa-0000-4000-8000-0000000000aa>",
               "WARC-Payload-Digest": "sha1:ENGLISHDIGESTDUMMYVALUE000000000"})
        target = self.arc_dir / "negotiation.warc"
        target.write_bytes(en_record + zh_revisit)
        self.index.index_paths([str(target)])
        result = self.index.get_by_record_id("urn:uuid:bbbbbbbb-0000-4000-8000-0000000000bb")
        self.assertFalse(result["resolution"]["resolved"])
        self.assertIn("内容协商", result["resolution"]["reason"])

    def test_digest_only_revisit_without_refers_to(self):
        import base64
        import hashlib
        payload = "摘要引用正文".encode()
        digest = "sha1:" + base64.b32encode(hashlib.sha1(payload).digest()).decode()
        response = build_record(
            "response", "cccccccc-0000-4000-8000-0000000000cc",
            b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(payload)).encode()
            + b"\r\n\r\n" + payload, **{"WARC-Payload-Digest": digest})
        revisit = build_record(
            "revisit", "dddddddd-0000-4000-8000-0000000000dd",
            b"HTTP/1.1 200 OK\r\n\r\n", warc_date="2026-11-04T00:00:00Z",
            **{"WARC-Payload-Digest": digest})  # 无 WARC-Refers-To
        target = self.arc_dir / "digest_only.warc"
        target.write_bytes(response + revisit)
        self.index.index_paths([str(target)])
        result = self.index.get_by_record_id("urn:uuid:dddddddd-0000-4000-8000-0000000000dd")
        self.assertTrue(result["resolution"]["resolved"])
        self.assertEqual(result["resolution"]["chain"][0]["via"], "payload-digest")

    def test_target_uri_date_evidence(self):
        response = build_record(
            "response", "eeeeeeee-0000-4000-8000-0000000000ee",
            b"HTTP/1.1 200 OK\r\n\r\nbody-x",
            **{"WARC-Date": "2026-10-01T00:00:00Z"})
        revisit = build_record(
            "revisit", "ffffffff-0000-4000-8000-0000000000ff",
            b"HTTP/1.1 200 OK\r\n\r\n", warc_date="2026-10-02T00:00:00Z",
            **{"WARC-Refers-To-Target-URI": URL,
               "WARC-Refers-To-Date": "2026-10-01T00:00:00Z"})
        target = self.arc_dir / "uri_date.warc"
        target.write_bytes(response + revisit)
        self.index.index_paths([str(target)])
        result = self.index.get_by_record_id("urn:uuid:ffffffff-0000-4000-8000-0000000000ff")
        self.assertTrue(result["resolution"]["resolved"])
        self.assertEqual(result["resolution"]["chain"][0]["via"], "target-uri+date")

    def test_insufficient_evidence_is_reported(self):
        revisit = build_record(
            "revisit", "11111111-0000-4000-8000-000000000011",
            b"HTTP/1.1 200 OK\r\n\r\n", warc_date="2026-11-05T00:00:00Z")
        target = self.arc_dir / "no_evidence.warc"
        target.write_bytes(revisit)
        self.index.index_paths([str(target)])
        result = self.index.get_by_record_id("urn:uuid:11111111-0000-4000-8000-000000000011")
        self.assertFalse(result["resolution"]["resolved"])
        self.assertIn("证据不充分", result["resolution"]["reason"])

    def test_multi_hop_chain_across_files(self):
        import base64
        import hashlib
        payload = b"chain-body"
        digest = "sha1:" + base64.b32encode(hashlib.sha1(payload).digest()).decode()
        r1 = build_record(
            "response", "a1a1a1a1-0000-4000-8000-0000000000a1",
            b"HTTP/1.1 200 OK\r\nContent-Language: zh\r\n\r\n" + payload,
            **{"WARC-Payload-Digest": digest})
        r2 = build_record(
            "revisit", "a2a2a2a2-0000-4000-8000-0000000000a2",
            b"HTTP/1.1 200 OK\r\nContent-Language: zh\r\n\r\n",
            warc_date="2026-11-02T00:00:00Z",
            **{"WARC-Refers-To": "<urn:uuid:a1a1a1a1-0000-4000-8000-0000000000a1>",
               "WARC-Payload-Digest": digest})
        r3 = build_record(
            "revisit", "a3a3a3a3-0000-4000-8000-0000000000a3",
            b"HTTP/1.1 200 OK\r\nContent-Language: zh\r\n\r\n",
            warc_date="2026-11-03T00:00:00Z",
            **{"WARC-Refers-To": "<urn:uuid:a2a2a2a2-0000-4000-8000-0000000000a2>",
               "WARC-Payload-Digest": digest})
        file_a = self.arc_dir / "chain_a.warc"
        file_b = self.arc_dir / "chain_b.warc"
        file_a.write_bytes(r1)
        file_b.write_bytes(r2 + r3)
        # revisit 链所在文件先入库，被引用 response 后到达。
        self.index.index_paths([str(file_b)])
        missing = self.index.get_by_record_id("urn:uuid:a3a3a3a3-0000-4000-8000-0000000000a3")
        self.assertFalse(missing["resolution"]["resolved"])
        self.index.index_paths([str(file_a)])
        resolved = self.index.get_by_record_id("urn:uuid:a3a3a3a3-0000-4000-8000-0000000000a3")
        self.assertTrue(resolved["resolution"]["resolved"])
        self.assertEqual([c["via"] for c in resolved["resolution"]["chain"]],
                         ["WARC-Refers-To", "WARC-Refers-To"])
        self.assertEqual(resolved["resolution"]["payload_record_id"],
                         "urn:uuid:a1a1a1a1-0000-4000-8000-0000000000a1")
        opened = self.index.open_record(resolved)
        self.assertEqual(opened["payload"], payload)
        self.assertEqual(opened["http_header_source"]["record_id"],
                         "urn:uuid:a3a3a3a3-0000-4000-8000-0000000000a3")


class IngestionTests(IndexHarness):
    def test_reindexing_is_idempotent(self):
        self.index.index_paths([self.p("originals.warc")])
        first = self.index.connection.execute("SELECT COUNT(*) c FROM records").fetchone()["c"]
        files_before = self.index.connection.execute("SELECT COUNT(*) c FROM files").fetchone()["c"]
        # 模拟入库被打断后重跑：同样文件不应重复登记。
        report = self.index.index_paths([self.p("originals.warc"), self.p("later.warc")])
        self.assertIn(self.p("originals.warc"), report.unchanged)
        report_two = self.index.index_paths([self.p("originals.warc"), self.p("later.warc")])
        self.assertEqual(report_two.unchanged,
                         [self.p("originals.warc"), self.p("later.warc")])
        second = self.index.connection.execute("SELECT COUNT(*) c FROM records").fetchone()["c"]
        files_after = self.index.connection.execute("SELECT COUNT(*) c FROM files").fetchone()["c"]
        self.assertEqual(second, first + 2)
        self.assertEqual(files_after, files_before + 1)

    def test_replaced_file_invalidates_old_offsets(self):
        target = self.arc_dir / "originals.warc"
        self.index.index_paths([str(target)])
        result = self.index.get_by_record_id(ORIGINAL_ID)
        self.assertEqual(result["location"]["record_offset"], 0)
        # 用不同内容替换同路径文件（模拟原文件被换掉）。
        replacement = build_record(
            "response", ORIGINAL_ID.split("urn:uuid:")[1],
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nnew!!")
        target.write_bytes(replacement)
        with self.assertRaises(StaleArchiveError):
            self.index.open_record(result)
        self.index.index_paths([str(target)])
        refreshed = self.index.get_by_record_id(ORIGINAL_ID)
        opened = self.index.open_record(refreshed)
        self.assertEqual(opened["payload"], b"new!!")
        # 旧文件行保留为 superseded，记录只挂在当前 ok 行上。
        statuses = [r["status"] for r in self.index.connection.execute(
            "SELECT status FROM files WHERE path=? ORDER BY file_id", (str(target),))]
        self.assertEqual(statuses, ["superseded", "ok"])
        orphan = self.index.connection.execute(
            "SELECT COUNT(*) c FROM records r JOIN files f ON r.file_id=f.file_id"
            " WHERE f.path=? AND f.status='superseded'", (str(target),)).fetchone()["c"]
        self.assertEqual(orphan, 0)

    def test_corruption_is_isolated(self):
        good = self.p("originals.warc")
        bad = self.p("truncated.warc")
        report = self.index.index_paths([bad, good])
        self.assertEqual(len(report.errors), 1)
        self.assertEqual(report.errors[0].path, bad)
        self.assertIn("截断", report.errors[0].reason)
        # 同批好文件照常入库可读。
        result = self.index.get_by_record_id(ORIGINAL_ID)
        self.assertEqual(result["warc_type"], "response")
        self.assertEqual(len(self.index.list_errors()), 1)
        # 损坏文件不登记任何记录行。
        count = self.index.connection.execute(
            "SELECT COUNT(*) c FROM records r JOIN files f ON r.file_id=f.file_id"
            " WHERE f.path=?", (bad,)).fetchone()["c"]
        self.assertEqual(count, 0)

    def test_corrupt_gzip_member_is_isolated(self):
        payload = build_record(
            "response", "99999999-0000-4000-8000-000000000099",
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
        good_blob = gzip.compress(payload, mtime=0)
        broken = good_blob[:-8] + b"\x00" * 8
        target = self.arc_dir / "broken.warc.gz"
        target.write_bytes(broken)
        report = self.index.index_paths([str(target), self.p("originals.warc")])
        self.assertEqual([e.path for e in report.errors], [str(target)])
        self.assertTrue(self.index.get_by_record_id(ORIGINAL_ID))

    def test_persistence_across_reopen(self):
        self.index.index_paths([self.p("originals.warc"), self.p("later.warc")])
        self.index.close()
        reopened = ArchiveIndex(self.db)
        result = reopened.get_by_record_id(REVISIT_ID)
        self.assertTrue(result["resolution"]["resolved"])
        reopened.close()
        self.index = ArchiveIndex(self.db)  # 防止 tearDown 关闭已关连接

    def test_interrupted_ingest_resumes_without_duplicates(self):
        # 第一批只入库一个文件，模拟入库进程在其余文件处理前被打断。
        self.index.index_paths([self.p("originals.warc")])
        self.index.close()
        self.index = ArchiveIndex(self.db)
        report = self.index.index_paths([
            self.p("originals.warc"), self.p("later.warc"),
            self.p("encoded.warc"), self.p("members.warc.gz")])
        self.assertEqual(report.unchanged, [self.p("originals.warc")])
        self.assertEqual(len(report.indexed), 3)
        # 再跑一遍全部文件：幂等，无重复登记。
        again = self.index.index_paths([
            self.p("originals.warc"), self.p("later.warc"),
            self.p("encoded.warc"), self.p("members.warc.gz")])
        self.assertEqual(len(again.unchanged), 4)
        self.assertEqual(again.indexed, [])
        distinct_files = self.index.connection.execute(
            "SELECT COUNT(DISTINCT path) c FROM files WHERE status='ok'").fetchone()["c"]
        self.assertEqual(distinct_files, 4)


class LookupTests(IndexHarness):
    def test_url_lookup_returns_all_representations(self):
        self.index.index_paths([
            self.p("originals.warc"), self.p("later.warc"), self.p("encoded.warc"),
            self.p("members.warc.gz")])
        results = self.index.find_by_url(URL)
        self.assertGreaterEqual(len(results), 7)
        languages = {r["http"]["content_language"] for r in results}
        self.assertIn("zh", languages)
        self.assertIn("en", languages)

    def test_url_lookup_nearest_at_time(self):
        self.index.index_paths([self.p("originals.warc"), self.p("later.warc")])
        nearest = self.index.find_by_url(URL, at="2026-11-02T12:00:00Z")
        self.assertEqual(len(nearest), 1)
        self.assertEqual(nearest[0]["record_id"], REVISIT_ID)
        # 精确落在某条捕获时间上时，该条即最近一次（含等号边界）。
        exact = self.index.find_by_url(URL, at="2026-11-02T00:00:00Z")
        self.assertEqual([r["record_id"] for r in exact], [REVISIT_ID])
        none_match = self.index.find_by_url(URL, at="2026-10-01T00:00:00Z")
        self.assertEqual(none_match, [])
        with self.assertRaises(ValueError):
            self.index.find_by_url(URL, at="not-a-date")

    def test_get_unknown_record_id(self):
        self.index.index_paths([self.p("originals.warc")])
        with self.assertRaises(KeyError):
            self.index.get_by_record_id("urn:uuid:00000000-0000-4000-8000-0000000000ff")


class ChunkedParsingTests(unittest.TestCase):
    def test_chunked_roundtrip_and_truncation(self):
        body = ws.decode_chunked(b"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n")
        self.assertEqual(body, b"Wikipedia")
        with self.assertRaises(ws.WarcFormatError):
            ws.decode_chunked(b"4\r\nWi")

    def test_gzip_member_splitting(self):
        blob = gzip.compress(b"one", mtime=0) + gzip.compress(b"two", mtime=0)
        members = list(ws.iter_gzip_members(blob))
        self.assertEqual([m.data for m in members], [b"one", b"two"])
        self.assertEqual(members[0].file_offset, 0)
        self.assertEqual(members[1].file_offset, members[0].compressed_span)
        with self.assertRaises(ws.WarcFormatError):
            list(ws.iter_gzip_members(b"\x1f\x8b\x08broken"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
