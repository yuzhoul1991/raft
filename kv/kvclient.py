from message.transport import Channel
import json
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR


class KvClient:
    def __init__(self, port):
        self._sock = socket(AF_INET, SOCK_STREAM)
        self._sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        self._sock.connect(("localhost", port))
        self._channel = Channel(self._sock)

    def set(self, key, val):
        self._channel.send_message(
            json.dumps({"op": "SET", "key": key, "val": val}).encode("utf-8")
        )

    def get(self, key):
        self._channel.send_message(
            json.dumps({"op": "GET", "key": key}).encode("utf-8")
        )
        return self._channel.recv_message().decode("utf-8")

    def delete(self, key):
        self._channel.send_message(
            json.dumps({"op": "DEL", "key": key}).encode("utf-8")
        )

    def close(self):
        self._sock.close()


def test_client(port):
    client = KvClient(port)
    client.set("foo", "hello")
    print("first set finished")
    client.set("bar", "world")
    print(client.get("foo"))
    client.delete("bar")
    client.close()


if __name__ == "__main__":
    import sys

    test_client(int(sys.argv[1]))
