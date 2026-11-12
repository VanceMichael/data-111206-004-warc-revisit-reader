import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from raw_store import RawStore


def demo(store):
    observed_at = datetime.now(timezone.utc).isoformat()
    import io
    from warc_source import records
    result=[]
    for name in ('originals.warc','later.warc'):
        raw=Path('fixtures',name).read_bytes()
        rows=list(records(io.BytesIO(raw)))
        digest=store.save(name,raw,{'record_count':len(rows)},observed_at)
        result.append({'文件':name,'记录数':len(rows),'类型':[row['headers']['warc-type'] for row in rows],'原文摘要':digest})
    return {'顺序读取':result,'说明':'重访载荷未解析，无随机读取索引'}


def main():
    parser = argparse.ArgumentParser(description='本地原文采集演示')
    parser.add_argument('command', choices=['demo'])
    parser.add_argument('--output', default='output/raw.sqlite3')
    args = parser.parse_args()
    store = RawStore(args.output)
    try:
        result = demo(store)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        store.close()


if __name__ == '__main__':
    main()
