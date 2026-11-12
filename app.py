import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# -I 隔离模式不会把脚本目录加入 sys.path，显式登记可信项目目录以导入本地模块。
sys.path.insert(0, str(Path(__file__).resolve().parent))

from archive_index import ArchiveIndex, StaleArchiveError
from raw_store import RawStore

DEFAULT_ARCHIVES = (
    "fixtures/originals.warc",
    "fixtures/later.warc",
    "fixtures/encoded.warc",
    "fixtures/members.warc.gz",
    "fixtures/truncated.warc",
)


def _print(payload):
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def cmd_index(args):
    index = ArchiveIndex(args.index)
    try:
        report = index.index_paths(args.archives)
        _print(report.as_dict())
        return 1 if report.errors else 0
    finally:
        index.close()


def cmd_find(args):
    index = ArchiveIndex(args.index)
    try:
        results = index.find_by_url(args.url, args.at)
        payload = []
        for result in results:
            payload.append(_summarize(result))
        _print({"url": args.url, "at": args.at, "count": len(payload), "captures": payload})
        return 0
    finally:
        index.close()


def cmd_get(args):
    index = ArchiveIndex(args.index)
    try:
        try:
            result = index.get_by_record_id(args.record_id)
        except KeyError:
            _print({"error": "not_found", "record_id": args.record_id})
            return 3
        opened = None
        if not args.metadata_only:
            opened = index.open_record(result)
        _print({"record": _summarize(result), "opened": _summarize_opened(opened)})
        return 0
    except StaleArchiveError as exc:
        _print({"error": "stale_archive", "reason": str(exc)})
        return 2
    finally:
        index.close()


def _summarize(result):
    summary = {
        "record_id": result["record_id"],
        "warc_type": result["warc_type"],
        "target_uri": result["target_uri"],
        "warc_date": result["warc_date"],
        "payload_digest": result["payload_digest"],
        "truncated": result["truncated"],
        "location": result["location"],
        "http": result["http"],
    }
    if result["resolution"] is not None:
        resolution = result["resolution"]
        summary["revisit"] = {
            "resolved": resolution["resolved"],
            "reason": resolution["reason"],
            "evidence": resolution["evidence"],
            "chain": resolution["chain"],
        }
        if resolution["resolved"]:
            summary["revisit"]["header_source"] = resolution["header_source"]
            summary["revisit"]["payload_source"] = resolution["payload_source"]
    return summary


def _summarize_opened(opened):
    if opened is None:
        return None
    payload = opened["payload"]
    return {
        "http_header_source": opened["http_header_source"],
        "payload_source": opened["payload_source"],
        "payload_length": None if payload is None else len(payload),
        "payload_preview": None if payload is None else payload[:80].decode("utf-8", "replace"),
        "decode_error": opened["decode_error"],
    }


def demo(store, index_db):
    """端到端演示：建索引、URL/时间查找、ID 读取、revisit 组合与损坏隔离。"""
    observed_at = datetime.now(timezone.utc).isoformat()
    index = ArchiveIndex(index_db)
    raw_observations = []
    try:
        for name in DEFAULT_ARCHIVES:
            raw = Path(name).read_bytes()
            digest = store.save(name, raw, {"ingested_by": "demo"}, observed_at)
            raw_observations.append({"file": name, "raw_digest": digest})
        report = index.index_paths(DEFAULT_ARCHIVES)
        captures = index.find_by_url("https://archive.example/product/1")
        original = index.get_by_record_id("urn:uuid:00000000-0000-4000-8000-000000000001")
        opened_original = index.open_record(original)
        revisit = index.get_by_record_id("urn:uuid:00000000-0000-4000-8000-000000000002")
        opened_revisit = index.open_record(revisit)
        loop = index.get_by_record_id("urn:uuid:00000000-0000-4000-8000-000000000003")
        bad_digest = index.get_by_record_id("urn:uuid:00000000-0000-4000-8000-000000000006")
        nearest = index.find_by_url(
            "https://archive.example/product/1", at="2026-11-02T12:00:00Z")
        return {
            "raw_observations": raw_observations,
            "index_report": report.as_dict(),
            "url_lookup_count": len(captures),
            "nearest_before_2026_11_02_noon": [_summarize(item) for item in nearest],
            "original_payload": opened_original["payload"].decode("utf-8"),
            "revisit_composed": {
                "resolved": revisit["resolution"]["resolved"],
                "http_header_from": opened_revisit["http_header_source"],
                "payload_from": opened_revisit["payload_source"],
                "payload_text": opened_revisit["payload"].decode("utf-8"),
            },
            "reference_loop": loop["resolution"],
            "digest_mismatch": bad_digest["resolution"],
        }
    finally:
        index.close()


def main():
    parser = argparse.ArgumentParser(description="授权 WARC 归档索引与随机读取")
    sub = parser.add_subparsers(dest="command", required=True)

    p_index = sub.add_parser("index", help="为归档建立或增量更新持久化索引")
    p_index.add_argument("archives", nargs="*", default=list(DEFAULT_ARCHIVES))
    p_index.add_argument("--index", default="output/archive_index.sqlite3")

    p_find = sub.add_parser("find", help="按 URL 与可选时间查找捕获")
    p_find.add_argument("url")
    p_find.add_argument("--at", help="ISO 8601 时间，给出不晚于该时间的最近捕获")
    p_find.add_argument("--index", default="output/archive_index.sqlite3")

    p_get = sub.add_parser("get", help="按 WARC-Record-ID 读取记录与组合载荷")
    p_get.add_argument("record_id")
    p_get.add_argument("--metadata-only", action="store_true")
    p_get.add_argument("--index", default="output/archive_index.sqlite3")

    p_demo = sub.add_parser("demo", help="本地合成数据端到端演示")
    p_demo.add_argument("--output", default="output/raw.sqlite3")
    p_demo.add_argument("--index", default="output/archive_index.sqlite3")

    args = parser.parse_args()
    if args.command == "index":
        raise SystemExit(cmd_index(args))
    if args.command == "find":
        raise SystemExit(cmd_find(args))
    if args.command == "get":
        raise SystemExit(cmd_get(args))

    store = RawStore(args.output)
    try:
        _print(demo(store, args.index))
    finally:
        store.close()


if __name__ == "__main__":
    main()
