from transport import Channel


from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR

sock = socket(AF_INET, SOCK_STREAM)
sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
sock.connect(("localhost", 12345))


def echo_test(sock):
    c = Channel(sock)
    for n in range(0, 8):
        msg = b"x" * (10 ** n)
        c.send_message(msg)
        response = c.recv_message()
        assert msg == response


echo_test(sock)
