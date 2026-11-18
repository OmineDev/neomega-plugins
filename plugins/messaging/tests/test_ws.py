"""Tests for the bounded RFC6455 OneBot WebSocket client."""
import asyncio
import base64
import hashlib
import json
import struct
import unittest

from ws import OneBotSocket


def _accept_key(key: str) -> str:
    return base64.b64encode(
        hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()
    ).decode()


def _server_frame(opcode: int, payload: bytes, *, final: bool = True) -> bytes:
    head = bytes([(0x80 if final else 0) | opcode])
    n = len(payload)
    if n < 126:
        head += bytes([n])
    elif n < 65536:
        head += bytes([126]) + struct.pack('!H', n)
    else:
        head += bytes([127]) + struct.pack('!Q', n)
    return head + payload


async def _read_client_frame(reader: asyncio.StreamReader):
    head = await reader.readexactly(2)
    opcode = head[0] & 0x0F
    length = head[1] & 0x7F
    if length == 126:
        length = struct.unpack('!H', await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack('!Q', await reader.readexactly(8))[0]
    mask = await reader.readexactly(4)
    masked = await reader.readexactly(length)
    payload = bytes(b ^ mask[i % 4] for i, b in enumerate(masked))
    return opcode, payload


class WebSocketClientTests(unittest.IsolatedAsyncioTestCase):
    async def _start_ws_server(self, handler):
        server = await asyncio.start_server(handler, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        return server, f'ws://127.0.0.1:{port}/'

    async def _handshake(self, reader, writer, *, connection_headers=('Connection: Upgrade',)):
        raw = await reader.readuntil(b'\r\n\r\n')
        lines = raw.decode('ascii').split('\r\n')
        headers = {k.strip().lower(): v.strip() for k, v in (l.split(':', 1) for l in lines[1:] if ':' in l)}
        accept = _accept_key(headers['sec-websocket-key'])
        response = [
            'HTTP/1.1 101 Switching Protocols',
            'Upgrade: websocket',
            *connection_headers,
            f'Sec-WebSocket-Accept: {accept}',
        ]
        writer.write(('\r\n'.join(response) + '\r\n\r\n').encode('ascii'))
        await writer.drain()

    async def test_connection_header_multi_token_variants(self):
        cases = [
            (['Connection: keep-alive, Upgrade'], True),
            (['Connection: keep-alive,Upgrade'], True),
            (['Connection: Keep-Alive ,  UPGRADE '], True),
            (['Connection: keep-alive', 'Connection: Upgrade'], True),
            (['Connection: keep-alive'], False),
            (['Connection: not-upgrade'], False),
        ]
        for header_lines, should_succeed in cases:
            with self.subTest(headers=header_lines):
                async def handle(reader, writer, h=header_lines):
                    try:
                        await self._handshake(reader, writer, connection_headers=h)
                    finally:
                        writer.close()
                        await writer.wait_closed()

                server, url = await self._start_ws_server(handle)
                sock = OneBotSocket(url, '')
                try:
                    if should_succeed:
                        await sock.connect()
                    else:
                        with self.assertRaises(ValueError):
                            await sock.connect()
                finally:
                    await sock.close()
                    server.close()
                    await server.wait_closed()

    async def test_idle_keepalive_ping_keeps_healthy_connection_alive(self):
        pings_seen = []

        async def handle(reader, writer):
            try:
                await self._handshake(reader, writer, connection_headers=('Connection: keep-alive, Upgrade',))
                for _ in range(2):
                    opcode, payload = await asyncio.wait_for(_read_client_frame(reader), 2)
                    if opcode == 9:
                        pings_seen.append(payload)
                        writer.write(_server_frame(10, payload))
                        await writer.drain()
                # After surviving multiple idle keepalive cycles, send an inbound message.
                event = {'post_type': 'message', 'message_type': 'group', 'group_id': 123, 'message_id': 1, 'message': 'hi'}
                writer.write(_server_frame(1, json.dumps(event).encode()))
                await writer.drain()
                writer.write(_server_frame(8, b''))
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        server, url = await self._start_ws_server(handle)
        sock = OneBotSocket(url, '', ping_interval=0.05, pong_timeout=0.2)
        inbox = asyncio.Queue()
        try:
            await sock.connect()
            await asyncio.wait_for(sock.receive(inbox), 2)
            self.assertEqual(len(pings_seen), 2)
            msg = await asyncio.wait_for(inbox.get(), 1)
            self.assertEqual(msg['message'], 'hi')
        finally:
            await sock.close()
            server.close()
            await server.wait_closed()

    async def test_idle_keepalive_drops_dead_peer_missing_pong(self):
        async def handle(reader, writer):
            try:
                await self._handshake(reader, writer)
                opcode, _ = await asyncio.wait_for(_read_client_frame(reader), 2)
                self.assertEqual(opcode, 9)
                # Intentionally never send pong.
                await asyncio.sleep(0.5)
            except (asyncio.CancelledError, ConnectionError, asyncio.IncompleteReadError):
                pass
            finally:
                writer.close()

        server, url = await self._start_ws_server(handle)
        sock = OneBotSocket(url, '', ping_interval=0.05, pong_timeout=0.05)
        inbox = asyncio.Queue()
        try:
            await sock.connect()
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(sock.receive(inbox), 2)
        finally:
            await sock.close()
            server.close()
            await server.wait_closed()


if __name__ == '__main__':
    unittest.main()
