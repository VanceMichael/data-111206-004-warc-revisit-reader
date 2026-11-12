import base64
import gzip
import hashlib
from pathlib import Path


def record(kind, identity, body, **extra):
    profile = {'WARC-Profile': 'http://netpreserve.org/warc/1.1/revisit/identical-payload-digest', 'WARC-Truncated': 'length'} if kind == 'revisit' else {}
    headers = {'WARC-Type': kind, 'WARC-Record-ID': f'<urn:uuid:{identity}>',
               'WARC-Date': '2026-11-01T00:00:00Z',
               'WARC-Target-URI': 'https://archive.example/product/1',
               'Content-Type': 'application/http; msgtype=response',
               **profile, **extra, 'Content-Length': str(len(body))}
    return ('WARC/1.1\r\n' + ''.join(f'{key}: {value}\r\n' for key, value in headers.items()) + '\r\n').encode() + body + b'\r\n\r\n'


def generate(directory='fixtures'):
    target = Path(directory)
    target.mkdir(parents=True, exist_ok=True)
    payload = '合成归档产品甲'.encode()
    digest = 'sha1:' + base64.b32encode(hashlib.sha1(payload).digest()).decode()
    original = record('response', '00000000-0000-4000-8000-000000000001',
                      b'HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: ' + str(len(payload)).encode() + b'\r\n\r\n' + payload,
                      **{'WARC-Payload-Digest': digest})
    revisit = record('revisit', '00000000-0000-4000-8000-000000000002',
                     b'HTTP/1.1 200 OK\r\nContent-Language: zh\r\n\r\n',
                     **{'WARC-Refers-To': '<urn:uuid:00000000-0000-4000-8000-000000000001>',
                        'WARC-Date': '2026-11-02T00:00:00Z', 'WARC-Payload-Digest': digest})
    loop = record('revisit', '00000000-0000-4000-8000-000000000003', b'HTTP/1.1 200 OK\r\n\r\n',
                  **{'WARC-Refers-To': '<urn:uuid:00000000-0000-4000-8000-000000000003>', 'WARC-Payload-Digest': digest})
    metadata = record('metadata', '00000000-0000-4000-8000-000000000004', '本地授权归档'.encode(), **{'Content-Type': 'text/plain; charset=utf-8'})
    (target / 'later.warc').write_bytes(revisit + loop)
    (target / 'originals.warc').write_bytes(original + metadata)
    (target / 'members.warc.gz').write_bytes(gzip.compress(original, mtime=0) + gzip.compress(revisit, mtime=0))
    (target / 'truncated.warc').write_bytes(original[:-15])
    packed = gzip.compress(payload, mtime=0)
    chunked = hex(len(packed))[2:].encode() + b'\r\n' + packed + b'\r\n0\r\n\r\n'
    packed_digest = 'sha1:' + base64.b32encode(hashlib.sha1(packed).digest()).decode()
    encoded = record('response', '00000000-0000-4000-8000-000000000005',
                     b'HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n' + chunked,
                     **{'WARC-Payload-Digest': packed_digest})
    bad_digest = record('revisit', '00000000-0000-4000-8000-000000000006',
                        b'HTTP/1.1 200 OK\r\nContent-Language: en\r\n\r\n',
                        **{'WARC-Refers-To': '<urn:uuid:00000000-0000-4000-8000-000000000001>',
                           'WARC-Payload-Digest': 'sha1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'})
    (target / 'encoded.warc').write_bytes(encoded + bad_digest)


if __name__ == '__main__':
    generate()
