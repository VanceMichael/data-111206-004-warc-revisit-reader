"""WARC/1.1 底层解析：顺序记录、逐成员 gzip、内嵌 HTTP 与摘要工具。

只依赖标准库。所有函数都接受 bytes，不触碰文件系统，便于对不可信归档
在隔离目录中按路径传入后解析。
"""

import base64
import gzip
import hashlib
import io
import re
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone

DEFAULT_LIMIT = 2 * 1024 * 1024
MAX_HEADER_BLOCK = 65536
GZIP_MAGIC = b"\x1f\x8b\x08"


class WarcFormatError(ValueError):
    """WARC 结构不符合 WARC/1.1 约定（含截断、非法头等）。"""


# ---------------------------------------------------------------------------
# 记录头与单记录解析
# ---------------------------------------------------------------------------

def parse_headers(buf, offset):
    """从 buf[offset:] 解析 WARC 记录头块，返回 (headers, header_end)。

    header_end 指向记录正文第一个字节（头块终止 CRLFCRLF 之后）。
    """
    if not buf.startswith(b"WARC/1.1", offset):
        raise WarcFormatError(f"归档版本错误，偏移 {offset}")
    line_end = buf.find(b"\r\n", offset)
    if line_end == -1:
        raise WarcFormatError(f"记录头截断，偏移 {offset}")
    headers = {}
    cursor = line_end + 2
    block_end = buf.find(b"\r\n\r\n", cursor)
    if block_end == -1 or block_end - offset > MAX_HEADER_BLOCK:
        raise WarcFormatError(f"记录头截断或过大，偏移 {offset}")
    while cursor < block_end:
        next_end = buf.find(b"\r\n", cursor)
        if next_end == -1 or next_end > block_end:
            raise WarcFormatError(f"记录头截断，偏移 {offset}")
        raw_line = buf[cursor:next_end]
        key, separator, value = raw_line.decode("utf-8", "strict").partition(":")
        if not separator or not key:
            raise WarcFormatError(f"非法记录头，偏移 {offset}")
        key = key.lower()
        if key in headers:
            raise WarcFormatError(f"重复记录头 {key}，偏移 {offset}")
        headers[key] = value.strip()
        cursor = next_end + 2
    return headers, block_end + 4


def parse_warc_record(buf, offset=0, limit=DEFAULT_LIMIT):
    """解析 buf 中从 offset 开始的一条 WARC 记录。

    返回 ScannedRecord，record_end 为含记录分隔 CRLFCRLF 后的下一条起点。
    """
    start = offset
    headers, body_start = parse_headers(buf, offset)
    try:
        length = int(headers["content-length"])
    except (KeyError, ValueError):
        raise WarcFormatError(f"缺少有效 Content-Length，偏移 {offset}") from None
    if length < 0 or length > limit:
        raise WarcFormatError(f"记录长度超出范围，偏移 {offset}")
    body_end = body_start + length
    if body_end > len(buf):
        raise WarcFormatError(f"记录正文截断，偏移 {offset}")
    record_end = body_end
    # WARC 记录后允许一个 CRLFCRLF 分隔块（fixtures 即此布局），逐字节跳过。
    if buf[record_end:record_end + 4] == b"\r\n\r\n":
        record_end += 4
    elif buf[record_end:record_end + 2] == b"\r\n":
        record_end += 2
    return ScannedRecord(
        offset=start,
        record_end=record_end,
        headers=headers,
        content=buf[body_start:body_end],
        compressed_offset=None,
        compressed_span=None,
        decompressed_length=None,
    )


@dataclass
class ScannedRecord:
    offset: int                      # 解压视角/纯文件内的记录起始字节偏移
    record_end: int                  # 下一条记录起点
    headers: dict                    # 小写键的 WARC 头
    content: bytes                   # Content-Length 声明长度的原始记录正文
    compressed_offset: int | None    # gzip 成员在归档文件中的字节偏移
    compressed_span: int | None      # gzip 成员在文件中占用的压缩字节数
    decompressed_length: int | None  # 该成员解压后的总字节数


def iter_warc_bytes(buf, limit=DEFAULT_LIMIT):
    """顺序遍历一段 bytes 中的全部 WARC 记录。"""
    for record in iter_warc_stream(io.BytesIO(buf), limit):
        yield record


def iter_warc_stream(handle, limit=DEFAULT_LIMIT):
    """流式顺序遍历文件对象中的 WARC 记录，避免把整份归档读入内存。

    偏移基于 handle.tell()，因此可直接作为文件字节偏移登记。
    """
    while True:
        offset = handle.tell()
        line = handle.readline()
        if not line:
            return
        if line in (b"\r\n", b"\n"):
            continue
        if line.rstrip() != b"WARC/1.1":
            raise WarcFormatError(f"归档版本错误，偏移 {offset}")
        headers = {}
        header_bytes = len(line)
        while True:
            current = handle.readline()
            if not current:
                raise WarcFormatError(f"记录头截断，偏移 {offset}")
            header_bytes += len(current)
            if header_bytes > MAX_HEADER_BLOCK:
                raise WarcFormatError(f"记录头过大，偏移 {offset}")
            if current in (b"\r\n", b"\n"):
                break
            key, separator, value = current.decode("utf-8", "strict").partition(":")
            if not separator or not key:
                raise WarcFormatError(f"非法记录头，偏移 {offset}")
            key = key.lower()
            if key in headers:
                raise WarcFormatError(f"重复记录头 {key}，偏移 {offset}")
            headers[key] = value.strip()
        try:
            length = int(headers["content-length"])
        except (KeyError, ValueError):
            raise WarcFormatError(f"缺少有效 Content-Length，偏移 {offset}") from None
        if length < 0 or length > limit:
            raise WarcFormatError(f"记录长度超出范围，偏移 {offset}")
        body = handle.read(length)
        if len(body) != length:
            raise WarcFormatError(f"记录正文截断，偏移 {offset}")
        record_end = handle.tell()
        separator = handle.read(4)
        if separator == b"\r\n\r\n":
            record_end = handle.tell()
        elif separator:
            handle.seek(-len(separator), 1)
        yield ScannedRecord(
            offset=offset,
            record_end=record_end,
            headers=headers,
            content=body,
            compressed_offset=None,
            compressed_span=None,
            decompressed_length=None,
        )


def records(stream, limit=DEFAULT_LIMIT):
    """按 WARC Content-Length 顺序读取未压缩记录流（保留旧接口）。"""
    while True:
        offset = stream.tell()
        line = stream.readline()
        if not line:
            return
        if line in (b"\r\n", b"\n"):
            continue
        if line.rstrip() != b"WARC/1.1":
            raise WarcFormatError(f"归档版本错误，偏移 {offset}")
        headers = {}
        header_size = 0
        while True:
            line = stream.readline()
            if not line:
                raise WarcFormatError("记录头截断")
            header_size += len(line)
            if header_size > MAX_HEADER_BLOCK:
                raise WarcFormatError("记录头过大")
            if line in (b"\r\n", b"\n"):
                break
            key, separator, value = line.decode("utf-8").partition(":")
            if not separator or key.lower() in headers:
                raise WarcFormatError("非法或重复记录头")
            headers[key.lower()] = value.strip()
        try:
            length = int(headers["content-length"])
        except (KeyError, ValueError):
            raise WarcFormatError("缺少有效 Content-Length") from None
        if length < 0 or length > limit:
            raise WarcFormatError("记录长度超出范围")
        body = stream.read(length)
        if len(body) != length:
            raise WarcFormatError("记录正文截断")
        yield {"offset": offset, "headers": headers, "body": body}
        # 跳过记录分隔 CRLFCRLF（若存在），与旧行为保持宽松一致。
        separator = stream.read(4)
        if separator and separator != b"\r\n\r\n":
            stream.seek(-len(separator), 1)


# ---------------------------------------------------------------------------
# 逐成员 gzip
# ---------------------------------------------------------------------------

@dataclass
class GzipMember:
    file_offset: int           # 成员在归档文件中的字节偏移
    compressed_span: int       # 成员占用的压缩字节数
    data: bytes                # 该成员解压后的完整 bytes


def iter_gzip_members(blob, limit=DEFAULT_LIMIT):
    """把逐记录 gzip（多成员）拆成 (文件偏移, 压缩跨度, 解压字节)。

    只解压当前成员，不触碰后续成员；成员损坏时抛出 WarcFormatError，
    由上层做损坏隔离。
    """
    position = 0
    while position < len(blob):
        if not blob.startswith(GZIP_MAGIC, position):
            raise WarcFormatError(f"gzip 成员魔数错误，文件偏移 {position}")
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
        try:
            data = decoder.decompress(blob[position:])
            data += decoder.flush()
        except zlib.error as exc:
            raise WarcFormatError(f"gzip 成员解压失败，文件偏移 {position}: {exc}") from None
        unused = decoder.unused_data
        if len(data) > limit:
            raise WarcFormatError(f"gzip 成员解压结果超出范围，文件偏移 {position}")
        compressed_span = len(blob) - position - len(unused)
        if not unused and position + compressed_span < len(blob):
            # 理论上不会发生：解压耗尽输入却仍有剩余字节。
            raise WarcFormatError(f"gzip 成员边界无法确定，文件偏移 {position}")
        yield GzipMember(position, compressed_span, data)
        position += compressed_span


def scan_compressed(blob, limit=DEFAULT_LIMIT):
    """解压单个 gzip 成员并扫描其中的 WARC 记录，附加成员定位信息。"""
    for member in iter_gzip_members(blob, limit):
        if len(member.data) == 0:
            raise WarcFormatError(f"gzip 成员为空，文件偏移 {member.file_offset}")
        found = 0
        for record in iter_warc_bytes(member.data, limit):
            record.compressed_offset = member.file_offset
            record.compressed_span = member.compressed_span
            record.decompressed_length = len(member.data)
            found += 1
            yield record
        if found == 0:
            raise WarcFormatError(f"gzip 成员内没有 WARC 记录，文件偏移 {member.file_offset}")


# ---------------------------------------------------------------------------
# 内嵌 HTTP 解析与编码解释
# ---------------------------------------------------------------------------

@dataclass
class HttpView:
    status_line: str
    headers: dict                       # 小写键
    raw_header_bytes: bytes             # 原始 HTTP 头字节（含状态行）
    entity: bytes                       # 去除 transfer-encoding 后的实体字节
    payload: bytes                      # 再解 content-encoding 后的载荷字节
    content_encodings: list = field(default_factory=list)
    transfer_encodings: list = field(default_factory=list)
    decode_error: str | None = None
    is_http: bool = True


def _split_header_lines(raw_headers):
    lines = raw_headers.split(b"\r\n")
    if len(lines) == 1:  # 宽容裸 \n 的非规范输入
        lines = raw_headers.split(b"\n")
    return lines


def parse_http_message(content):
    """解释 application/http 记录正文，返回 HttpView；非 HTTP 内容标记 is_http=False。"""
    split = content.find(b"\r\n\r\n")
    if split == -1:
        if content.find(b"\n\n") != -1:
            split = content.find(b"\n\n")
            separator = 2
        else:
            return HttpView("", {}, b"", content, content, is_http=False)
    else:
        separator = 4
    raw_headers = content[:split]
    entity = content[split + separator:]
    lines = _split_header_lines(raw_headers)
    status_line = lines[0].decode("iso-8859-1") if lines else ""
    if not re.match(r"^HTTP/\d(\.\d)? \d{3} ", status_line):
        return HttpView("", {}, b"", entity, entity, is_http=False)
    headers = {}
    for line in lines[1:]:
        if not line:
            continue
        text = line.decode("iso-8859-1")
        key, _, value = text.partition(":")
        if key:
            headers[key.strip().lower()] = value.strip()
    transfer = [part.strip().lower() for part in headers.get("transfer-encoding", "").split(",") if part.strip()]
    encodings = [part.strip().lower() for part in headers.get("content-encoding", "").split(",") if part.strip()]
    decode_error = None
    try:
        if "chunked" in transfer:
            entity = decode_chunked(entity)
        payload = decode_content(entity, encodings)
    except WarcFormatError as exc:
        decode_error = str(exc)
        payload = b""
    return HttpView(
        status_line=status_line,
        headers=headers,
        raw_header_bytes=raw_headers,
        entity=entity,
        payload=payload,
        content_encodings=encodings,
        transfer_encodings=transfer,
        decode_error=decode_error,
    )


def decode_chunked(blob):
    """解码 HTTP/1.1 chunked transfer-encoding。"""
    output = bytearray()
    cursor = 0
    while True:
        line_end = blob.find(b"\r\n", cursor)
        if line_end == -1:
            raise WarcFormatError("chunked 正文截断：缺少分块长度行")
        size_text = blob[cursor:line_end].split(b";", 1)[0].strip()
        try:
            size = int(size_text, 16)
        except ValueError:
            raise WarcFormatError(f"非法 chunk 长度: {size_text!r}") from None
        cursor = line_end + 2
        if size == 0:
            # 尾部头之后应以 CRLF 结束；允许直接结束。
            tail_end = blob.find(b"\r\n", cursor)
            if tail_end == -1:
                if blob[cursor:].strip() == b"":
                    break
                raise WarcFormatError("chunked 正文截断：缺少结尾 CRLF")
            cursor = tail_end + 2
            break
        if cursor + size > len(blob):
            raise WarcFormatError("chunked 正文截断：分块数据不完整")
        output += blob[cursor:cursor + size]
        cursor += size
        if blob[cursor:cursor + 2] != b"\r\n":
            raise WarcFormatError("chunked 分块缺少结尾 CRLF")
        cursor += 2
    return bytes(output)


def decode_content(entity, encodings):
    """按 Content-Encoding 顺序逐层解码（gzip/deflate；其余标准库不支持则报错）。"""
    payload = entity
    for encoding in encodings:
        if encoding == "gzip" or encoding == "x-gzip":
            try:
                payload = gzip.decompress(payload)
            except (OSError, EOFError, zlib.error) as exc:
                raise WarcFormatError(f"gzip 内容解码失败: {exc}") from None
        elif encoding == "deflate":
            try:
                payload = zlib.decompress(payload)
            except zlib.error:
                try:
                    payload = zlib.decompress(payload, -zlib.MAX_WBITS)
                except zlib.error as exc:
                    raise WarcFormatError(f"deflate 内容解码失败: {exc}") from None
        elif encoding in ("br", "zstd", "compress", "identity"):
            raise WarcFormatError(f"标准库不支持的 Content-Encoding: {encoding}")
        else:
            raise WarcFormatError(f"未知 Content-Encoding: {encoding}")
    return payload


# ---------------------------------------------------------------------------
# WARC 标识、日期与摘要
# ---------------------------------------------------------------------------

def normalize_record_id(value):
    """去掉 <urn:uuid:...> 外壳，返回裸 URN；非法输入返回 None。"""
    if not value:
        return None
    text = value.strip()
    match = re.fullmatch(r"<(.+)>", text)
    if match:
        text = match.group(1)
    if text.startswith("urn:uuid:") or text.startswith("<"):
        return text
    return text if re.match(r"^[^\s:]+:[^\s]+$", text) else None


def normalize_digest(value):
    """把 WARC 摘要归一为 (algorithm, base32 大写) 二元组；无法识别返回 None。"""
    if not value:
        return None
    text = value.strip().lower()
    if ":" not in text:
        return None
    algorithm, encoded = text.split(":", 1)
    encoded = encoded.strip().upper()
    if algorithm not in ("sha1", "sha256", "md5"):
        return None
    if not re.fullmatch(r"[A-Z2-7]+=*", encoded):
        return None
    return algorithm, encoded


def compute_digest(algorithm, payload):
    """按 WARC 约定计算 base32 摘要（不带算法前缀）。"""
    hasher = hashlib.new(algorithm)
    hasher.update(payload)
    return base64.b32encode(hasher.digest()).decode("ascii")


def digests_match(declared, actual_algorithm, actual_bytes):
    """校验声明摘要与实际字节；声明缺失时按实际算法现算并返回归一摘要。"""
    actual_b32 = compute_digest(actual_algorithm, actual_bytes)
    declared_norm = normalize_digest(declared)
    if declared_norm is None:
        return None, (actual_algorithm, actual_b32)
    algorithm, encoded = declared_norm
    if algorithm != actual_algorithm:
        # 算法不同无法直接比对，现算声明算法再比。
        expected_b32 = compute_digest(algorithm, actual_bytes)
        return expected_b32 == encoded, (algorithm, expected_b32)
    return encoded == actual_b32, (actual_algorithm, actual_b32)


def parse_warc_date(value):
    """解析 WARC-Date（ISO 8601，UTC Z）；失败返回 None。"""
    if not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
