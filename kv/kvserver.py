from message.transport import Channel
import json
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR


class KvServer:
    def __init__(self, port):
        self._sock = socket(AF_INET, SOCK_STREAM)
        self._sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        self._sock.bind(("localhost", port))
        print("KV Server listening on port ", port)
        self._sock.listen(1)
        self._data = {}

    def _get(self, key):
        if key not in self._data:
            return ""
        return self._data[key]

    def _set(self, key, val):
        self._data[key] = val

    def _del(self, key):
        del self._data[key]

    def _handle_request(self, encoded_request: bytes):
        request = json.loads(encoded_request)
        op = request["op"]
        key = request["key"]
        if op == "GET":
            return self._get(key)
        elif op == "SET":
            self._set(key, request["val"])
            return ""
        elif op == "DEL":
            self._del(key)
            return ""

    def start(self):
        while True:
            client, addr = self._sock.accept()
            print("KV Server connected with address: ", addr)
            c = Channel(client)
            while True:
                request = c.recv_message()
                if not request:
                    break
                print("request", request)
                response = self._handle_request(request.decode("utf-8"))
                if response:
                    c.send_message(response.encode("utf-8"))
            client.close()


def run_server(port):
    server = KvServer(port)
    server.start()


if __name__ == "__main__":
    import sys

    run_server(int(sys.argv[1]))
