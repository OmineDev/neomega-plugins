"""Small bounded RFC6455 client for the existing OneBot forward WS gateway."""
import asyncio
import base64
import hashlib
import json
import os
import ssl
import struct
from urllib.parse import urlsplit


class OneBotSocket:
    def __init__(self, endpoint, token):
        self.endpoint, self.token = endpoint, token
        self.reader = self.writer = None
        self.lock = asyncio.Lock()
        self.pending = {}
        self.sequence = 0

    async def connect(self):
        url = urlsplit(self.endpoint)
        secure = url.scheme == 'wss'
        self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(
            url.hostname, url.port or (443 if secure else 80),
            ssl=ssl.create_default_context() if secure else None, limit=8192), 10)
        key = base64.b64encode(os.urandom(16)).decode()
        host = url.hostname if ':' not in url.hostname else '[' + url.hostname + ']'
        if url.port:
            host += ':' + str(url.port)
        headers = [f'GET {url.path or "/"} HTTP/1.1', f'Host: {host}', 'Upgrade: websocket',
                   'Connection: Upgrade', 'Sec-WebSocket-Version: 13', 'Sec-WebSocket-Key: ' + key]
        if self.token:
            if '\r' in self.token or '\n' in self.token:
                raise ValueError('invalid token')
            headers.append('Authorization: Bearer ' + self.token)
        self.writer.write(('\r\n'.join(headers) + '\r\n\r\n').encode('ascii'))
        await self.writer.drain()
        raw = await asyncio.wait_for(self.reader.readuntil(b'\r\n\r\n'), 10)
        lines = raw.decode('ascii').split('\r\n')
        fields = {a.strip().lower(): b.strip() for a, b in (line.split(':', 1) for line in lines[1:] if ':' in line)}
        accept = base64.b64encode(hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
        if (len(raw) > 8192 or lines[0].split()[1] != '101' or fields.get('sec-websocket-accept') != accept
                or fields.get('upgrade', '').lower() != 'websocket'
                or 'upgrade' not in fields.get('connection', '').lower().split(', ')):
            raise ValueError('websocket handshake rejected')

    async def frame(self, opcode, payload):
        if len(payload) > 65536:
            raise ValueError('frame exceeds bridge limit')
        mask = os.urandom(4)
        length = len(payload)
        head = bytes([128 | opcode, 128 | length]) if length < 126 else bytes([128 | opcode, 254]) + struct.pack('!H', length) if length < 65536 else bytes([128 | opcode, 255]) + struct.pack('!Q', length)
        async with self.lock:
            self.writer.write(head + mask + bytes(c ^ mask[i % 4] for i, c in enumerate(payload)))
            await self.writer.drain()

    async def call(self, method, arguments):
        if self.writer is None or self.writer.is_closing():
            raise ConnectionError('websocket disconnected')
        self.sequence += 1
        echo = 'neomega_' + str(self.sequence)
        future = asyncio.get_running_loop().create_future()
        self.pending[echo] = future
        try:
            await self.frame(1, json.dumps({'action': method, 'params': arguments, 'echo': echo}, ensure_ascii=False).encode())
            return await asyncio.wait_for(future, 10)
        finally:
            self.pending.pop(echo, None)

    async def receive(self, inbox):
        fragmented = bytearray()
        fragment_opcode = None
        while True:
            head = await asyncio.wait_for(self.reader.readexactly(2), 90)
            final, opcode, masked, length = bool(head[0] & 128), head[0] & 15, head[1] & 128, head[1] & 127
            if head[0] & 112 or masked:
                raise ValueError('invalid server frame')
            if length == 126:
                length = struct.unpack('!H', await self.reader.readexactly(2))[0]
            elif length == 127:
                length = struct.unpack('!Q', await self.reader.readexactly(8))[0]
            if length > 65536 or (opcode >= 8 and (length > 125 or not final)):
                raise ValueError('frame exceeds bridge limit')
            data = await asyncio.wait_for(self.reader.readexactly(length), 10)
            if opcode == 8:
                return
            if opcode == 9:
                await self.frame(10, data)
                continue
            if opcode == 10:
                continue
            if opcode in (1, 2):
                if fragment_opcode is not None:
                    raise ValueError('interleaved fragments')
                fragment_opcode = opcode
            elif opcode != 0 or fragment_opcode is None:
                raise ValueError('invalid continuation')
            fragmented.extend(data)
            if len(fragmented) > 65536:
                raise ValueError('message exceeds bridge limit')
            if not final:
                continue
            message = bytes(fragmented)
            kind = fragment_opcode
            fragmented.clear()
            fragment_opcode = None
            if kind != 1:
                continue
            row = json.loads(message)
            if not isinstance(row, dict):
                continue
            future = self.pending.get(str(row.get('echo', '')))
            if future is not None and not future.done():
                future.set_result(row)
            elif row.get('post_type') == 'message':
                # Bounded event queue; saturation backpressures the socket.
                await inbox.put(row)

    async def close(self):
        for future in self.pending.values():
            if not future.done():
                future.set_exception(ConnectionError('websocket disconnected'))
        if self.writer is not None:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except OSError:
                pass
