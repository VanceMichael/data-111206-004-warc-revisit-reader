"""可持久化的 WARC 归档索引与随机读取。

职责
----
1. 扫描未压缩 WARC 与逐记录 gzip 成员，登记文件字节偏移、压缩成员跨度与
   解压后长度，随机读取只打开对应成员，不解压整批归档。
2. 解释内嵌 HTTP 的 transfer/content 编码，保留原始字节位置与捕获时间。
3. 解析 revisit：WARC-Refers-To、目标 URI+日期、载荷摘要三条证据，
   明确处理跨文件、后到达记录、引用环与摘要不符；证据不足给出不可解析原因，
   不访问公网，也不用另一个语言/内容协商表示顶替。
4. 入库按文件事务化，中断重跑不重复登记；原文件被替换（大小/摘要变化）时
   旧偏移整体失效。损坏归档隔离登记，不影响同批其他文件。

仅使用 Python 标准库。
"""

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import warc_source as ws

SCHEMA_VERSION = 1
WARC_TYPES = ("response", "revisit", "metadata")

# 同一路径保留全部历史文件行，file_id 最大者为当前行；只有当前 ok 行可查。
ACTIVE_FILE = (
    "f.status='ok' AND f.file_id="
    "(SELECT max(f2.file_id) FROM files f2 WHERE f2.path=f.path)")


class StaleArchiveError(RuntimeError):
    """索引中的偏移对不上当前文件（文件被替换或删除），需要重新索引。"""


@dataclass
class ScanError:
    path: str
    reason: str
    offset: int | None = None


@dataclass
class IndexReport:
    indexed: list = field(default_factory=list)        # [(path, record_count)]
    unchanged: list = field(default_factory=list)      # [path]
    errors: list = field(default_factory=list)         # [ScanError]

    def as_dict(self):
        return {
            "indexed": [{"file": p, "records": n} for p, n in self.indexed],
            "unchanged": self.unchanged,
            "errors": [
                {"file": e.path, "reason": e.reason, **({"offset": e.offset} if e.offset is not None else {})}
                for e in self.errors
            ],
        }


# ---------------------------------------------------------------------------
# 扫描：文件 -> 记录行
# ---------------------------------------------------------------------------

def _sha256_file(path):
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def scan_archive(path, limit=ws.DEFAULT_LIMIT):
    """扫描单个归档，返回 (row_dicts, ScanError|None)。

    未压缩 WARC 流式扫描；gzip 按成员读取。出错时返回已解析行，
    由调用方在事务中整体丢弃。
    """
    path = Path(path)
    compressed = path.name.endswith(".gz")
    rows = []
    try:
        if compressed:
            with path.open("rb") as handle:
                blob = handle.read()
            iterator = ws.scan_compressed(blob, limit)
            for record in iterator:
                rows.append(_row_from_record(record))
        else:
            with path.open("rb") as handle:
                for record in ws.iter_warc_stream(handle, limit):
                    rows.append(_row_from_record(record))
    except (ws.WarcFormatError, UnicodeDecodeError, OSError) as exc:
        return rows, ScanError(str(path), str(exc), getattr(exc, "offset", None))
    return [row for row in rows if row is not None], None


def _row_from_record(record):
    headers = record.headers
    warc_type = headers.get("warc-type", "")
    if warc_type not in WARC_TYPES:
        return None
    http = None
    http_status = None
    http_headers_json = None
    entity_length = None
    payload_length = None
    http_decode_error = None
    content_type = headers.get("content-type", "")
    if content_type.startswith("application/http"):
        http = ws.parse_http_message(record.content)
        http_status = http.status_line
        http_headers_json = json.dumps(http.headers, ensure_ascii=False, sort_keys=True)
        entity_length = len(http.entity)
        payload_length = len(http.payload)
        http_decode_error = http.decode_error
    return {
        "record_id": ws.normalize_record_id(headers.get("warc-record-id")),
        "warc_type": warc_type,
        "target_uri": headers.get("warc-target-uri"),
        "warc_date": headers.get("warc-date"),
        "date_norm": _date_text(headers.get("warc-date")),
        "payload_digest": headers.get("warc-payload-digest"),
        "refers_to": ws.normalize_record_id(headers.get("warc-refers-to")),
        "refers_to_target_uri": headers.get("warc-refers-to-target-uri"),
        "refers_to_date": headers.get("warc-refers-to-date"),
        "profile": headers.get("warc-profile"),
        "truncated": headers.get("warc-truncated"),
        "content_type": content_type,
        "record_offset": record.offset,
        "record_end": record.record_end,
        "content_length": len(record.content),
        "compressed_offset": record.compressed_offset,
        "compressed_span": record.compressed_span,
        "decompressed_length": record.decompressed_length,
        "http_status": http_status,
        "http_headers": http_headers_json,
        "entity_length": entity_length,
        "payload_length": payload_length,
        "http_decode_error": http_decode_error,
        "content_language": (http.headers.get("content-language") if http else None),
    }


def _date_text(value):
    parsed = ws.parse_warc_date(value)
    return parsed.isoformat() if parsed else None


# ---------------------------------------------------------------------------
# SQLite 持久化索引
# ---------------------------------------------------------------------------

class ArchiveIndex:
    def __init__(self, db_path):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(db_path, timeout=10)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS files (
                file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL,
                size INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                compressed INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('ok','error','superseded')),
                error_reason TEXT,
                indexed_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS records (
                record_pk INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER NOT NULL REFERENCES files(file_id),
                record_id TEXT,
                warc_type TEXT NOT NULL,
                target_uri TEXT,
                warc_date TEXT,
                date_norm TEXT,
                payload_digest TEXT,
                refers_to TEXT,
                refers_to_target_uri TEXT,
                refers_to_date TEXT,
                profile TEXT,
                truncated TEXT,
                content_type TEXT,
                record_offset INTEGER NOT NULL,
                record_end INTEGER NOT NULL,
                content_length INTEGER NOT NULL,
                compressed_offset INTEGER,
                compressed_span INTEGER,
                decompressed_length INTEGER,
                http_status TEXT,
                http_headers TEXT,
                entity_length INTEGER,
                payload_length INTEGER,
                http_decode_error TEXT,
                content_language TEXT);
            CREATE INDEX IF NOT EXISTS idx_records_record_id ON records(record_id);
            CREATE INDEX IF NOT EXISTS idx_records_url_date ON records(target_uri, date_norm);
            CREATE INDEX IF NOT EXISTS idx_records_digest ON records(payload_digest);
        """)
        self.connection.execute(
            "INSERT INTO schema_meta(key,value) VALUES('version',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),))
        self.connection.commit()

    def close(self):
        self.connection.close()

    # -- 入库 ---------------------------------------------------------------

    def index_paths(self, paths):
        report = IndexReport()
        for raw_path in paths:
            path = str(Path(raw_path))
            try:
                stat = Path(path).stat()
                digest = _sha256_file(path)
            except OSError as exc:
                self._insert_error(path, f"无法读取文件: {exc}")
                report.errors.append(ScanError(path, f"无法读取文件: {exc}"))
                continue
            current = self._current_file(path)
            if current is not None and current["sha256"] == digest and current["status"] == "ok":
                report.unchanged.append(path)
                continue
            rows, error = scan_archive(path)
            indexed_at = datetime.now(timezone.utc).isoformat()
            # 每个文件一个事务：中断后重跑不会留下半文件，也不会重复登记。
            with self.connection:
                if current is not None:
                    self.connection.execute("DELETE FROM records WHERE file_id=?", (current["file_id"],))
                    self.connection.execute(
                        "UPDATE files SET status='superseded' WHERE file_id=?", (current["file_id"],))
                if error is not None:
                    self.connection.execute(
                        "INSERT INTO files(path,size,sha256,compressed,mtime_ns,status,error_reason,indexed_at)"
                        " VALUES(?,?,?,?,?,?,?,?)",
                        (path, stat.st_size, digest, 1 if path.endswith(".gz") else 0,
                         stat.st_mtime_ns, "error", error.reason, indexed_at))
                    report.errors.append(error)
                else:
                    cursor = self.connection.execute(
                        "INSERT INTO files(path,size,sha256,compressed,mtime_ns,status,error_reason,indexed_at)"
                        " VALUES(?,?,?,?,?,?,?,?)",
                        (path, stat.st_size, digest, 1 if path.endswith(".gz") else 0,
                         stat.st_mtime_ns, "ok", None, indexed_at))
                    self._insert_rows(cursor.lastrowid, rows)
                    report.indexed.append((path, len(rows)))
        return report

    def _current_file(self, path):
        return self.connection.execute(
            "SELECT * FROM files WHERE path=? ORDER BY file_id DESC LIMIT 1", (path,)).fetchone()

    def _insert_error(self, path, reason):
        indexed_at = datetime.now(timezone.utc).isoformat()
        with self.connection:
            current = self._current_file(path)
            if current is not None:
                self.connection.execute("DELETE FROM records WHERE file_id=?", (current["file_id"],))
                self.connection.execute(
                    "UPDATE files SET status='superseded' WHERE file_id=?", (current["file_id"],))
            self.connection.execute(
                "INSERT INTO files(path,size,sha256,compressed,mtime_ns,status,error_reason,indexed_at)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (path, -1, "", 0, 0, "error", reason, indexed_at))

    def _insert_rows(self, file_id, rows):
        columns = [
            "file_id", "record_id", "warc_type", "target_uri", "warc_date", "date_norm",
            "payload_digest", "refers_to", "refers_to_target_uri", "refers_to_date",
            "profile", "truncated", "content_type", "record_offset", "record_end",
            "content_length", "compressed_offset", "compressed_span", "decompressed_length",
            "http_status", "http_headers", "entity_length", "payload_length",
            "http_decode_error", "content_language"]
        placeholders = ",".join("?" for _ in columns)
        payload = [tuple([file_id] + [row[name] for name in columns[1:]]) for row in rows]
        self.connection.executemany(
            f"INSERT INTO records ({','.join(columns)}) VALUES ({placeholders})", payload)

    # -- 查询入口 ------------------------------------------------------------

    def find_by_url(self, url, at=None):
        """按 URL 查找捕获，at 为 ISO 时间时返回不晚于该时间的最近一次（否则全部）。

        同一 URL 的多语言/内容协商表示都会返回，由 content_language 区分，
        不做跨表示顶替。
        """
        rows = self.connection.execute(
            "SELECT r.*, f.path AS file_path, f.status AS file_status, f.sha256 AS file_sha256,"
            " f.size AS file_size, f.mtime_ns AS file_mtime_ns"
            " FROM records r JOIN files f ON r.file_id=f.file_id"
            f" WHERE {ACTIVE_FILE} AND r.target_uri=?"
            " ORDER BY COALESCE(r.date_norm,''), r.record_pk", (url,)).fetchall()
        if at is not None:
            target = ws.parse_warc_date(at)
            if target is None:
                raise ValueError(f"非法时间: {at}")
            target_text = target.isoformat()
            eligible = [row for row in rows if row["date_norm"] and row["date_norm"] <= target_text]
            rows = eligible[-1:] if eligible else []
        return [self._resolve(row) for row in rows]

    def get_by_record_id(self, record_id):
        """按记录 ID 读取。重复 ID 取最早入库的有效行，全部位置随结果返回。"""
        normalized = ws.normalize_record_id(record_id) or record_id
        rows = self.connection.execute(
            "SELECT r.*, f.path AS file_path, f.status AS file_status, f.sha256 AS file_sha256,"
            " f.size AS file_size, f.mtime_ns AS file_mtime_ns"
            " FROM records r JOIN files f ON r.file_id=f.file_id"
            f" WHERE {ACTIVE_FILE} AND r.record_id=?"
            " ORDER BY r.record_pk", (normalized,)).fetchall()
        if not rows:
            raise KeyError(record_id)
        return self._resolve(rows[0], duplicate_rows=rows[1:])

    def list_errors(self):
        return [dict(row) for row in self.connection.execute(
            "SELECT path,error_reason,indexed_at FROM files f"
            " WHERE status='error' AND file_id=(SELECT max(f2.file_id) FROM files f2 WHERE f2.path=f.path)"
            " ORDER BY path")]

    # -- revisit 解析 --------------------------------------------------------

    def _resolve(self, row, duplicate_rows=None, _seen=None):
        """把索引行扩展为解析结果；revisit 在此追链，所有判断只基于本地索引。"""
        result = {
            "record_id": row["record_id"],
            "warc_type": row["warc_type"],
            "target_uri": row["target_uri"],
            "warc_date": row["warc_date"],
            "payload_digest": row["payload_digest"],
            "truncated": row["truncated"],
            "location": self._location(row),
            "http": {
                "status": row["http_status"],
                "headers": json.loads(row["http_headers"]) if row["http_headers"] else {},
                "entity_length": row["entity_length"],
                "payload_length": row["payload_length"],
                "decode_error": row["http_decode_error"],
                "content_language": row["content_language"],
            },
            "resolution": None,
            "duplicate_locations": [self._location(other) for other in (duplicate_rows or [])],
        }
        if row["warc_type"] != "revisit":
            return result
        if _seen is None:
            _seen = set()
        result["resolution"] = self._resolve_revisit(row, _seen | {row["record_pk"]})
        return result

    def _resolve_revisit(self, row, seen):
        target = self._find_revisit_target(row)
        # 注意 sqlite3.Row 是 tuple 子类，因此按长度区分“原因单元素元组”与 (row, kind)。
        if isinstance(target, tuple) and len(target) == 1:
            return {"resolved": False, "reason": target[0], "chain": [], "evidence": self._evidence(row)}
        target_row, evidence_kind = target
        chain = [{
            "record_id": row["record_id"],
            "file": row["file_path"],
            "via": evidence_kind,
        }]
        if target_row["record_pk"] in seen:
            return {"resolved": False,
                    "reason": f"引用环：{row['record_id']} -> {target_row['record_id']}",
                    "chain": chain, "evidence": self._evidence(row)}
        if target_row["warc_type"] == "revisit":
            sub = self._resolve_revisit(target_row, seen | {target_row["record_pk"]})
            chain.extend(sub["chain"])
            if not sub["resolved"]:
                sub["chain"] = chain
                return sub
            payload_row_pk = sub["payload_record_pk"]
        else:
            payload_row_pk = target_row["record_pk"]
        payload_row = self.connection.execute(
            "SELECT r.*, f.path AS file_path, f.status AS file_status, f.sha256 AS file_sha256,"
            " f.size AS file_size, f.mtime_ns AS file_mtime_ns"
            " FROM records r JOIN files f ON r.file_id=f.file_id"
            " WHERE r.record_pk=?", (payload_row_pk,)).fetchone()
        digest_check = self._check_digest(row, payload_row)
        if digest_check is not None:
            return {"resolved": False, "reason": digest_check, "chain": chain,
                    "evidence": self._evidence(row)}
        # 内容协商保护：语言/媒体类型不同的表示不得顶替。
        negotiation = self._negotiation_conflict(row, payload_row)
        if negotiation is not None:
            return {"resolved": False, "reason": negotiation, "chain": chain,
                    "evidence": self._evidence(row)}
        return {
            "resolved": True,
            "reason": None,
            "chain": chain,
            "payload_record_pk": payload_row_pk,
            "payload_record_id": payload_row["record_id"],
            "payload_location": self._location(payload_row),
            "header_source": {
                "record_id": row["record_id"], "file": row["file_path"],
                "note": "HTTP 头取自 revisit 记录自身"},
            "payload_source": {
                "record_id": payload_row["record_id"], "file": payload_row["file_path"],
                "note": "载荷体取自被引用记录，摘要已校验一致"},
            "evidence": self._evidence(row),
        }

    def _find_revisit_target(self, row):
        """按三种证据找目标，返回 (row, evidence_kind) 或 (原因字符串,)。"""
        # 证据 1：WARC-Refers-To（跨文件、后到达都由索引覆盖）。
        if row["refers_to"]:
            candidates = self._active_rows(
                "record_id=? ORDER BY record_pk", (row["refers_to"],))
            if not candidates:
                return (f"WARC-Refers-To 指向的记录 {row['refers_to']} 在本地索引中不存在",)
            chosen = self._disambiguate(candidates, row)
            if chosen is None:
                return (f"WARC-Refers-To 命中 {len(candidates)} 条同 ID 记录且无法唯一确定",)
            return chosen, "WARC-Refers-To"
        # 证据 2：WARC-Refers-To-Target-URI + WARC-Refers-To-Date。
        if row["refers_to_target_uri"] and row["refers_to_date"]:
            date_text = _date_text(row["refers_to_date"])
            candidates = self._active_rows(
                "target_uri=? AND date_norm=? ORDER BY record_pk",
                (row["refers_to_target_uri"], date_text))
            if not candidates:
                return ("WARC-Refers-To 目标 URI/日期在本地索引中无匹配",)
            chosen = self._disambiguate(candidates, row)
            if chosen is None:
                return ("WARC-Refers-To 目标 URI/日期命中多条记录，无法唯一确定",)
            return chosen, "target-uri+date"
        # 证据 3：载荷摘要 + 同 URL（含仅写摘要的 revisit）。
        if row["payload_digest"]:
            candidates = self._active_rows(
                "warc_type='response' AND target_uri=? AND payload_digest=? ORDER BY record_pk",
                (row["target_uri"], row["payload_digest"]))
            if not candidates:
                return ("载荷摘要在同 URL 的 response 记录中无匹配",)
            chosen = self._disambiguate(candidates, row)
            if chosen is None:
                return ("载荷摘要命中多条 response 且内容协商表示无法唯一确定",)
            return chosen, "payload-digest"
        return ("revisit 缺少 WARC-Refers-To、目标日期和载荷摘要，证据不充分",)

    def _disambiguate(self, candidates, revisit_row):
        """同 ID/同摘要多副本时确定性挑选：同 URL -> response 优先 -> 同语言 -> 最早捕获。

        全部条件仍无法收敛为 1 条时返回 None，交由上层报不可解析，绝不随机顶替。
        """
        pool = list(candidates)
        same_url = [c for c in pool if c["target_uri"] == revisit_row["target_uri"]]
        if same_url:
            pool = same_url
        if len(pool) == 1:
            return pool[0]
        responses = [c for c in pool if c["warc_type"] == "response"]
        if responses:
            pool = responses
        if len(pool) == 1:
            return pool[0]
        wanted_lang = (revisit_row["content_language"] or "").lower()
        if wanted_lang:
            same_lang = [c for c in pool if (c["content_language"] or "").lower() == wanted_lang]
            if same_lang:
                pool = same_lang
        if len(pool) == 1:
            return pool[0]
        # 同内容多副本（如同一记录同时存在于 WARC 与 warc.gz）：选最早入库的一条，
        # 其余副本仍可通过 duplicate_locations 查到，不丢证据。
        pool.sort(key=lambda c: c["record_pk"])
        return pool[0]

    def _active_rows(self, where, params):
        return list(self.connection.execute(
            "SELECT r.*, f.path AS file_path, f.status AS file_status, f.sha256 AS file_sha256,"
            " f.size AS file_size, f.mtime_ns AS file_mtime_ns"
            " FROM records r JOIN files f ON r.file_id=f.file_id"
            f" WHERE {ACTIVE_FILE} AND {where}", params))

    def _check_digest(self, revisit_row, payload_row):
        """摘要不符时返回原因字符串，一致返回 None。"""
        declared = ws.normalize_digest(revisit_row["payload_digest"])
        target_declared = ws.normalize_digest(payload_row["payload_digest"])
        if declared is None:
            return None  # revisit 未声明摘要：交由调用方按其他证据处理，不伪造校验。
        if target_declared is None:
            return "被引用记录缺少可比对的载荷摘要，无法确认正文一致性"
        if declared != target_declared:
            return (
                f"载荷摘要不符：revisit 声明 {revisit_row['payload_digest']}，"
                f"被引用记录为 {payload_row['payload_digest']}")
        return None

    def _negotiation_conflict(self, revisit_row, payload_row):
        lang_a = (revisit_row["content_language"] or "").lower()
        lang_b = (payload_row["content_language"] or "").lower()
        if lang_a and lang_b and lang_a != lang_b:
            return f"内容协商表示冲突：revisit 语言 {lang_a}，候选载荷语言 {lang_b}，拒绝跨表示顶替"
        return None

    def _evidence(self, row):
        return {
            "refers_to": row["refers_to"],
            "refers_to_target_uri": row["refers_to_target_uri"],
            "refers_to_date": row["refers_to_date"],
            "payload_digest": row["payload_digest"],
        }

    # -- 随机读取 ------------------------------------------------------------

    def read_record_bytes(self, record_pk=None, location=None):
        """按主键或 location 字典从归档读取单条 WARC 记录的原始正文。

        压缩归档只 seek 到对应 gzip 成员并解压该成员，不触碰其他成员。
        """
        if location is None:
            row = self.connection.execute(
                "SELECT r.*, f.path AS file_path, f.status AS file_status, f.sha256 AS file_sha256,"
                " f.size AS file_size, f.mtime_ns AS file_mtime_ns, f.compressed AS compressed"
                " FROM records r JOIN files f ON r.file_id=f.file_id"
                " WHERE r.record_pk=?", (record_pk,)).fetchone()
            if row is None:
                raise KeyError(record_pk)
        else:
            row = location
        self._verify_file_current(row)
        path = Path(row["file_path"])
        if row["compressed_offset"] is not None:
            with self._open(path) as handle:
                handle.seek(row["compressed_offset"])
                member_blob = handle.read(row["compressed_span"])
            members = list(ws.iter_gzip_members(member_blob))
            if len(members) != 1:
                raise StaleArchiveError(
                    f"{path} 偏移 {row['compressed_offset']} 处不是单个 gzip 成员，索引已失效")
            if len(members[0].data) != row["decompressed_length"]:
                raise StaleArchiveError(
                    f"{path} 成员解压长度与索引不符，索引已失效")
            record = ws.parse_warc_record(members[0].data, row["record_offset"])
        else:
            with self._open(path) as handle:
                handle.seek(row["record_offset"])
                raw = handle.read(row["record_end"] - row["record_offset"])
            record = ws.parse_warc_record(raw, 0)
        if len(record.content) != row["content_length"]:
            raise StaleArchiveError(f"{path} 记录 Content-Length 与索引不符，索引已失效")
        return record.content

    def _open(self, path):
        """文件打开接缝，测试可替换为记录读取范围的实现。"""
        return open(path, "rb")

    def open_record(self, resolved):
        """读取解析结果对应的载荷。

        response/metadata：本记录正文（HTTP 已解释时返回解码后 payload 及头来源）。
        已解析 revisit：HTTP 头来自 revisit 自身，载荷来自被引用记录，
        分别按需读取各自成员，并再次现算摘要核对。
        """
        location = resolved["location"]
        row = self._row_by_location(location)
        if row is None:
            raise StaleArchiveError("索引行已不存在，需要重新索引")
        content = self.read_record_bytes(location=row)
        opened = {
            "record_id": resolved["record_id"],
            "warc_type": resolved["warc_type"],
            "raw_content": content,
            "http_header_source": {"record_id": resolved["record_id"], "file": row["file_path"]},
            "payload_source": {"record_id": resolved["record_id"], "file": row["file_path"]},
            "payload": None,
            "entity": None,
            "http_headers": resolved["http"]["headers"],
            "decode_error": resolved["http"]["decode_error"],
        }
        if row["content_type"].startswith("application/http"):
            view = ws.parse_http_message(content)
            opened["entity"] = view.entity
            opened["payload"] = view.payload
        else:
            opened["payload"] = content
            opened["entity"] = content
        resolution = resolved.get("resolution")
        if resolution and resolution.get("resolved"):
            payload_row = self._row_by_location(resolution["payload_location"])
            if payload_row is None:
                raise StaleArchiveError("被引用记录的索引行已不存在，需要重新索引")
            target_content = self.read_record_bytes(location=payload_row)
            target_view = ws.parse_http_message(target_content)
            declared_norm = ws.normalize_digest(row["payload_digest"])
            algorithm = declared_norm[0] if declared_norm else "sha1"
            ok, actual = ws.digests_match(row["payload_digest"], algorithm, target_view.payload)
            if ok is False:
                raise StaleArchiveError(
                    f"读取时现算摘要与 revisit 声明不符（实际 {actual[0]}:{actual[1]}），拒绝拼接")
            opened["payload"] = target_view.payload
            opened["entity"] = target_view.entity
            opened["payload_source"] = {
                "record_id": payload_row["record_id"], "file": payload_row["file_path"],
                "digest": f"{actual[0]}:{actual[1]}",
                "note": "载荷取自被引用记录；HTTP 头仍来自 revisit 记录"}
            opened["http_header_source"] = {
                "record_id": row["record_id"], "file": row["file_path"],
                "note": "revisit 自身的 HTTP 头（含 Content-Language 等协商信息）"}
        return opened

    def _row_by_location(self, location):
        return self.connection.execute(
            "SELECT r.*, f.path AS file_path, f.status AS file_status, f.sha256 AS file_sha256,"
            " f.size AS file_size, f.mtime_ns AS file_mtime_ns, f.compressed AS compressed"
            " FROM records r JOIN files f ON r.file_id=f.file_id"
            " WHERE f.path=? AND r.record_offset=? AND COALESCE(r.compressed_offset,-1)=COALESCE(?,-1)",
            (location["file"], location["record_offset"], location.get("compressed_offset"))).fetchone()

    def _verify_file_current(self, row):
        if row["file_status"] != "ok" or row["file_id"] != self.connection.execute(
                "SELECT max(file_id) FROM files WHERE path=?", (row["file_path"],)).fetchone()[0]:
            raise StaleArchiveError(f"{row['file_path']} 的该索引行已失效，请重新索引")
        path = Path(row["file_path"])
        if not path.exists():
            raise StaleArchiveError(f"{row['file_path']} 已被删除，索引失效")
        stat = path.stat()
        if stat.st_size != row["file_size"]:
            raise StaleArchiveError(f"{row['file_path']} 大小变化，旧偏移失效，请重新索引")
        current_digest = _sha256_file(path)
        if current_digest != row["file_sha256"]:
            raise StaleArchiveError(f"{row['file_path']} 内容摘要变化，旧偏移失效，请重新索引")

    @staticmethod
    def _location(row):
        return {
            "file": row["file_path"],
            "record_offset": row["record_offset"],
            "compressed_offset": row["compressed_offset"],
            "compressed_span": row["compressed_span"],
            "decompressed_length": row["decompressed_length"],
            "content_length": row["content_length"],
        }
