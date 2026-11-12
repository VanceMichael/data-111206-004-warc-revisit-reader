def records(stream, limit=2 * 1024 * 1024):
    while True:
        offset = stream.tell()
        line = stream.readline()
        if not line:
            return
        if line in (b'\r\n', b'\n'):
            continue
        if line.rstrip() != b'WARC/1.1':
            raise ValueError(f'归档版本错误，偏移 {offset}')
        headers = {}
        header_size = 0
        while True:
            line = stream.readline()
            header_size += len(line)
            if not line or header_size > 65536:
                raise ValueError('记录头截断或过大')
            if line in (b'\r\n', b'\n'):
                break
            key, separator, value = line.decode('utf-8').partition(':')
            if not separator or key.lower() in headers:
                raise ValueError('非法或重复记录头')
            headers[key.lower()] = value.strip()
        try:
            length = int(headers['content-length'])
        except (KeyError, ValueError):
            raise ValueError('缺少有效 Content-Length') from None
        if length < 0 or length > limit:
            raise ValueError('记录长度超出范围')
        body = stream.read(length)
        if len(body) != length:
            raise ValueError('记录正文截断')
        yield {'offset': offset, 'headers': headers, 'body': body}
