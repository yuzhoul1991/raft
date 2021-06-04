from raft_server import *
import network
import random
from log import *
from itertools import permutations

# deliver the message for each network and clear the outbox
def deliver_single_message(src_net, des_net):
    assert des_net.addr in src_net.peers
    if not src_net.outbox[des_net.addr].empty():
        message = src_net.outbox[des_net.addr].get()
        des_net.inbox.put(message)
        return True
    else:
        return False


# deliver any messages for a random permutations of server nets
# This simulates the randomness of network including random delay relative to
# the server operations
# return true if there is a message getting consumed
def deliver_message(servers, dead_nodes=None):
    if not any(
        server.net.has_pending_messages(
            [peer.id for peer in servers if peer.id != server.id]
        )
        for server in servers
    ):
        return False

    nets = [server.net for server in servers]

    src, des = random.choice(list(permutations(nets, 2)))
    while not deliver_single_message(src, des):
        src, des = random.choice(list(permutations(nets, 2)))
    return True


# create an interconnected server cluster of |size|
def create_cluster(size):
    return [
        network.RaftNet(addr, [i for i in range(size) if i != addr])
        for addr in range(size)
    ]


def iterate(servers):
    deliver_message(servers)
    for server in servers:
        server.handle_incoming_messages()


def simulate(servers):
    while any(
        server.net.has_pending_messages(
            [peer.id for peer in servers if peer.id != server.id]
        )
        for server in servers
    ):
        iterate(servers)


@attr.s
class PersistState:
    current_term = attr.ib()
    log = attr.ib()
    voted_for = attr.ib()


@attr.s
class Simulator:
    size = attr.ib(default=3)
    servers = attr.ib(init=False)
    persistent_state = attr.ib(init=False)
    timeouts = attr.ib(init=False)
    dead_nodes = attr.ib(init=False)

    def __attrs_post_init__(self):
        # create some empty servers
        self.servers = [
            RaftServer(net=net, log=raft_log.RaftLog())
            for net in create_cluster(self.size)
        ]
        # maintain persistent_state of servers
        self._save_persistent_state()
        self.timeouts = {
            server.id: Timeout(peers=server.net.peers) for server in self.servers
        }
        self.dead_nodes = set()

    def _save_persistent_state(self):
        self.persistent_state = {
            server.id: PersistState(
                current_term=server.current_term,
                log=server.log,
                voted_for=server.voted_for,
            )
            for server in self.servers
        }

    def kill_server(self, server):
        addr = server.id
        nets = [server.net for server in self.servers]
        for net in nets:
            if net.addr == addr:
                net.disable(net.peers)
            else:
                net.disable([addr])
        server.state = State.Dead

    def resurrect_server(self, addr):
        for idx, server in enumerate(self.servers):
            if server.id == addr:
                self.servers[idx] = self.load_persistent_state(addr)

        nets = [server.net for server in self.servers]
        for net in nets:
            if net.addr != addr:
                net.enable([addr])

    # given the dead server, returns a new server with the persistent state populated
    def load_persistent_state(self, addr):
        state = self.persistent_state[addr]
        return RaftServer(
            net=network.RaftNet(
                addr, peers=[s.id for s in self.servers if s.id != addr]
            ),
            current_term=state.current_term,
            log=state.log,
            voted_for=state.voted_for,
        )

    def _decrement_timeout(self):
        for server in self.servers:
            if server.state in [State.Follower, State.Candidate]:
                if server.timeout.decrement_election_timeout():
                    print(f"Server:{server.id} election timeout")
                    server.update_state(State.Candidate)
            else:
                # leader
                if server.timeout.decrement_heartbeat_timeout():
                    print(f"Server:{server.id} heartbeat timeout")
                    server.send_heart_beat()

    # one iteration, representing one unit of time passage
    def iterate(self, time):
        self._decrement_timeout()
        if deliver_message(self.servers, self.dead_nodes):
            # print("Time: ", time)
            # print([server.net for server in self.servers])
            print({server.id: server.state for server in self.servers})
            # print("\n")

        for server in self.servers:
            server.handle_incoming_messages()

        self._save_persistent_state()
        # return the servers for checking
        return self.servers


def basic_test():
    # setup a network, check in simulation once a leader is established, it shouldn't change
    sim = Simulator(5)
    leader = None
    for i in range(5000):
        servers = sim.iterate(i)
        any_leader = [server for server in servers if server.state == State.Leader]
        new_leader = any_leader[0] if any_leader else None
        if any_leader:
            assert len(any_leader) == 1

        if leader is None:
            leader = new_leader

        assert leader == new_leader

    assert leader.current_term == 1


def get_leader(servers):
    maybe_leader = [server for server in servers if server.state == State.Leader]
    assert len(maybe_leader) in [0, 1]
    if len(maybe_leader) == 1:
        return maybe_leader[0]
    return None


def check_log_replication(servers):
    leader = get_leader(servers)
    assert leader is not None
    assert all(
        follower.log == leader.log for follower in servers if follower.id != leader.id
    )


def basic_log_replication():
    sim = Simulator(5)
    leader = None
    for time in range(50000):
        servers = sim.iterate(time)
        any_leader = [server for server in servers if server.state == State.Leader]
        new_leader = any_leader[0] if any_leader else None
        if any_leader:
            assert len(any_leader) == 1

        if leader is None:
            leader = new_leader

        # insert a couple of log entries to leader
        if time % 1000 == 0 and leader is not None:
            leader.leader_append_entries(
                [
                    LogEntry(leader.current_term),
                    LogEntry(leader.current_term),
                    LogEntry(leader.current_term),
                ]
            )

        if leader is not None:
            assert new_leader is not None

        assert leader == new_leader
    assert leader.current_term == 1
    check_log_replication(servers)


def leader_death_and_comeback():
    sim = Simulator(5)
    for time in range(100000):
        servers = sim.iterate(time)
        leader = get_leader(servers)

        # insert a couple of log entries to leader
        if time == 10000:
            assert leader is not None
            leader.leader_append_entries(
                [
                    LogEntry(leader.current_term),
                    LogEntry(leader.current_term),
                    LogEntry(leader.current_term),
                ]
            )

        if time == 20000:
            assert leader is not None
            leader.leader_append_entries(
                [
                    LogEntry(leader.current_term),
                    LogEntry(leader.current_term),
                    LogEntry(leader.current_term),
                ]
            )

        if time == 30000:
            assert leader is not None
            sim.kill_server(leader)

        if time == 40000:
            assert leader is not None
            to_resurrect = [
                server.id for server in servers if server.state == State.Dead
            ][0]
            sim.resurrect_server(to_resurrect)

    # one leader is expected to get re-elected and log replicated
    assert leader is not None
    check_log_replication(servers)


if __name__ == "__main__":
    # basic_test()
    # basic_log_replication()
    leader_death_and_comeback()
