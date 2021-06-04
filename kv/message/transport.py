import struct


class Channel:
    def __init__(self, sock):
        self._sock = sock
        self._size_struct = struct.Struct("I")
        self._buffer = bytearray()

    def _encode(self, msg: bytes) -> bytes:
        packed_size = self._size_struct.pack(len(msg))
        return packed_size + msg

    def _decode(self, encoded_msg: bytes) -> (bytes, int):
        # encoded_msg not contain complete size prefix
        size_len = self._size_struct.size
        if len(encoded_msg) < size_len:
            return (b"", 0)
        size = self._size_struct.unpack(encoded_msg[:size_len])[0]
        # encoded_msg is incomplete
        if len(encoded_msg) < size_len + size:
            return (b"", 0)
        # normal case with a complete msg
        return (encoded_msg[size_len : size_len + size], size_len + size)

    def send_message(self, msg: bytes):
        self._sock.sendall(self._encode(msg))

    def recv_message(self):
        msg, consumed = self._decode(bytes(self._buffer))
        while not msg:
            chunk = self._sock.recv(10000)
            if not chunk:
                return None
            self._buffer.extend(chunk)
            msg, consumed = self._decode(bytes(self._buffer))
        del self._buffer[:consumed]
        return msg
