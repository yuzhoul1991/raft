from transport import Channel

from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR

sock = socket(AF_INET, SOCK_STREAM)
sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
sock.bind(("", 12345))
sock.listen(1)


def echo_server(sock):
    # keep the loop running forever, in case there are more than one clients
    while True:
        client, addr = sock.accept()
        print("connected with addr: ", addr)
        c = Channel(client)
        while True:
            msg = c.recv_message()
            if not msg:
                break
            c.send_message(msg)
        client.close()


echo_server(sock)
