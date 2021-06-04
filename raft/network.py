import queue
import attr

from typing import Optional


@attr.s
class Message:
    src = attr.ib()
    des = attr.ib()
    data = attr.ib(default=None)


@attr.s(repr=False)
class RaftNet:
    addr = attr.ib()
    peers = attr.ib()
    inbox = attr.ib(init=False)
    outbox = attr.ib(init=False)
    disabled = attr.ib(init=False)

    def __attrs_post_init__(self):
        self.inbox = queue.Queue()
        self.outbox = {peer: queue.Queue() for peer in self.peers}
        self.disabled = set()

    def __repr__(self):
        outgoing = {peer: q.qsize() for peer, q in self.outbox.items()}
        return f"RaftNet(address={self.addr}, incoming={self.inbox.qsize()}, outgoing={outgoing})"

    # receive is non-blocking
    def receive(self) -> Optional[Message]:
        if self.inbox.empty():
            return None
        # blocks untill the inbox queue has an entry
        return self.inbox.get()

    def send(self, des, data=None, call_back=None):
        if des in self.disabled:
            return None
        data.des = des
        # print(data)
        message = Message(self.addr, des, data)
        self.outbox[des].put(message)
        if call_back:
            call_back(des, message)

    def has_pending_messages(self, peers):
        return (not self.inbox.empty()) or any(
            [not out_q.empty() for peer, out_q in self.outbox.items() if peer in peers]
        )

    def disable(self, peers):
        for peer in peers:
            self.disabled.add(peer)

    def enable(self, peers):
        for peer in peers:
            if peer in self.disabled:
                self.disabled.remove(peer)


def basic_test():
    net = RaftNet(0, [1, 2])
    assert net.receive() is None
    net.send(1)
    assert not net.outbox[1].empty()

    print(net)

    net.disable(1)
    assert net.send(1) is None
    net.enable(1)
    net.outbox[1] = queue.Queue()
    net.send(1)
    assert not net.outbox[1].empty()

    print("pass basic test")


if __name__ == "__main__":
    basic_test()
