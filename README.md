# 授权网页归档顺序查看器

在原有 WARC/1.1 顺序读取之上，增加**可持久化的归档索引与随机读取**，让查看器
能够离线打开 response / revisit / metadata 记录，包括只写“正文沿用旧版本”的
revisit 页面。全部数据为 `make_archives.py` 生成的本地合成归档，不含第三方网页
正文或外部附件，运行期不访问公网。

## 运行

Python 3.11+、Linux、仅标准库。索引与原文默认落在 `output/`（已 gitignore），
删除该目录即可回到干净状态。

```sh
# 全部单元测试（37 个），使用 -I 隔离模式
python3 -I -m unittest discover -s tests -v
python3 -I -m compileall -q .

# 端到端演示：建索引、URL/时间查找、ID 读取、revisit 组合、损坏隔离
python3 -I app.py demo
```

## 命令行入口

```sh
# 1) 建索引 / 增量补索引（可传任意未压缩 .warc 或逐成员 .warc.gz）
python3 -I app.py index --index output/archive_index.sqlite3 fixtures/originals.warc fixtures/later.warc

# 2) 按 URL 查找；--at 给出不晚于指定时间的最近一次捕获
python3 -I app.py find 'https://archive.example/product/1'
python3 -I app.py find 'https://archive.example/product/1' --at 2026-11-02T12:00:00Z

# 3) 按 WARC-Record-ID 读取，输出 HTTP 头来源、载荷来源与载荷预览
python3 -I app.py get 'urn:uuid:00000000-0000-4000-8000-000000000002'
```

退出码：`0` 成功；`1` 索引批次中存在被隔离的损坏文件；`2` 索引偏移已失效
（原文件被替换/删除，需重新索引）；`3` 记录 ID 不存在。

## 模块

- `warc_source.py`：无文件系统依赖的底层解析。
  - WARC/1.1 记录头/正文解析，流式（`iter_warc_stream`）与 bytes
    （`iter_warc_bytes`、旧接口 `records`）两种遍历，偏移即文件字节偏移。
  - `iter_gzip_members` 按 gzip 成员边界拆分，返回成员文件偏移、压缩跨度与
    解压后长度；随机读取只 seek 并解压目标成员，不解压整批归档。
  - 内嵌 HTTP 解析：先去 `Transfer-Encoding: chunked`，再逐层解
    `Content-Encoding`（gzip/deflate，标准库外的编码明确报错而非乱猜），
    同时保留原始 HTTP 头字节与 WARC 捕获时间、字节位置。
  - WARC-Record-ID / WARC-Date / base32 载荷摘要（sha1/sha256/md5）工具。
- `archive_index.py`：SQLite 持久化索引（`ArchiveIndex`）。
- `raw_store.py`：原始字节原文库（SHA-256 去重），与索引库相互独立。
- `app.py`：`index` / `find` / `get` / `demo` 命令。

## 索引数据模型

`files` 表按路径保留全部历史文件行（同路径 `file_id` 最大者为当前行）：
`size`、整文件 `sha256`、`mtime_ns`、`status`（ok/error/superseded）与
错误原因。`records` 表登记每条记录的 WARC 头字段、捕获时间归一值、
`record_offset`/`record_end`、gzip 的 `compressed_offset`/`compressed_span`/
`decompressed_length`、HTTP 状态与头、实体/载荷长度及解码错误。

## revisit 解析规则

只使用本地索引证据，按顺序尝试：

1. `WARC-Refers-To` 指向的记录 ID（支持跨文件、被引用记录后到达——补索引后
   旧 revisit 自动可解析，无需改写）；
2. `WARC-Refers-To-Target-URI` + `WARC-Refers-To-Date`；
3. 同 URL 且 `WARC-Payload-Digest` 一致的 response。

同一证据命中多副本时按“同 URL → response 优先 → 同 Content-Language →
最早入库”确定性收敛；无法唯一确定则报不可解析。每种失败都给出明确原因：
引用目标缺失、引用环、摘要不符、内容协商语言冲突、证据不充分等。

组合结果严格区分来源：**HTTP 头取自 revisit 记录自身，载荷取自被引用记录**；
拼接前比对声明摘要，`open_record` 读取时还会现算摘要二次核对。摘要不符或
语言表示不同（如 zh revisit 指向 en response）时拒绝拼接，绝不借公网补正文，
也不用另一个协商表示顶替。

## 入库可靠性与损坏隔离

- 每个文件一个 SQLite 事务：进程中断不会留下半文件；重跑同样文件走
  `unchanged` 分支，不重复登记。
- 原文件被替换（大小或整文件 SHA-256 变化）：旧文件行标记 `superseded`、
  旧记录行删除，旧偏移立即失效；读取失效位置抛 `StaleArchiveError`，
  重新索引后按新位置读取。
- 截断 WARC、损坏 gzip 成员只隔离该文件（`status=error` 并登记原因），
  同批其他文件正常入库可读。

## 环境

`requirements.txt` 登记精确依赖（当前仅标准库）。环境准备阶段安装依赖，
运行、构建及测试不执行下载。
