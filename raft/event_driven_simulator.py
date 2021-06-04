"""
This simulator is different from the simulator.py as it is a event driven as apposed
to time driven.
"""
import attr
import time
import random
import threading
import logging
from raft_server import *
from enum import Enum
from rpc import HeartBeat, Election
from simulator import PersistState
from itertools import permutations
from simulator import create_cluster
from threading import Thread
from log import LogEntry


@attr.s
class EventDrivenSimulator:
    HEARTBEAT_TIMER = 1
    ELECTION_TIMER = 5
    ELECTION_RANDOM = 2
    MESSAGE_DELAY = 0.5

    # runtime in real time in seconds
    run_time = attr.ib()
    size = attr.ib(default=3)
    servers = attr.ib(init=False)
    time_ran = attr.ib(init=False, default=0)
    killed = attr.ib(init=False)
    persistent_state = attr.ib(init=False)

    def __attrs_post_init__(self):
        self.servers = [
            RaftServer(net=net, log=raft_log.RaftLog())
            for net in create_cluster(self.size)
        ]
        self.killed = set()
        self._save_persistent_state()

    def _save_persistent_state(self):
        self.persistent_state = {
            server.id: PersistState(
                current_term=server.current_term,
                log=server.log,
                voted_for=server.voted_for,
            )
            for server in self.servers
        }

    def _should_stop(self):
        if self.time_ran > self.run_time:
            return True
        return False

    def monitor(self):
        while True:
            if self._should_stop():
                return
            monitor_internal = 2
            time.sleep(monitor_internal)
            self.time_ran += monitor_internal
            logging.info(f"Monitor[TimeRan]={self.time_ran}")
            logging.info(
                f"Monitor[State]:        {[server.state for server in self.servers]}"
            )
            for server in self.servers:
                logging.info(
                    f"Monitor[Log]:          Server={server.id} Term={server.current_term} LogSize={len(server.log)}"
                )

    def _find_leader(self):
        leaders = [server for server in self.servers if server.state == State.Leader]
        assert len(leaders) in [0, 1]
        if len(leaders) == 1:
            return leaders[0]
        return None

    def client(self):
        while True:
            if self._should_stop():
                return
            time.sleep(7)
            # if there is a leader, send a append entry command
            leader = self._find_leader()
            if leader is not None:
                leader.leader_append_entries([LogEntry(leader.current_term)])

    def _kill_server(self, server):
        addr = server.id
        nets = [server.net for server in self.servers]
        for net in nets:
            if net.addr == addr:
                net.disable(net.peers)
            else:
                net.disable([addr])
        server.state = State.Dead
        self.killed.add(server.id)

    # given the dead server, returns a new server with the persistent state populated
    def _load_persistent_state(self, addr):
        state = self.persistent_state[addr]
        return RaftServer(
            net=network.RaftNet(
                addr, peers=[s.id for s in self.servers if s.id != addr]
            ),
            current_term=state.current_term,
            log=state.log,
            voted_for=state.voted_for,
        )

    def _resurrect_server(self, addr):
        for idx, server in enumerate(self.servers):
            if server.id == addr:
                self.servers[idx] = self._load_persistent_state(addr)

        nets = [server.net for server in self.servers]
        for net in nets:
            if net.addr != addr:
                net.enable([addr])

    def mess_with_leader(self):
        while True:
            if self._should_stop():
                return
            time.sleep(25)
            # if killed, resurrect it
            if len(self.killed) > 0:
                self._resurrect_server(self.killed.pop())
                continue

            leader = self._find_leader()
            if leader is not None:
                self._save_persistent_state()
                self._kill_server(leader)

    # For each server, we run an election timer, if the timer times out, generate
    # such an event and put into the server's inbox.
    def election_timer(self, server):
        while True:
            if self._should_stop():
                return
            if server.state == State.Dead:
                continue
            time.sleep(self.ELECTION_TIMER + random.random() * self.ELECTION_RANDOM)
            server.net.inbox.put(Election())

    # For a leader server, run a heartbeat timer, if the timer times out, generate
    # such an event and put into the server's inbox.
    def heartbeat_timer(self, server):
        while True:
            if self._should_stop():
                return
            time.sleep(self.HEARTBEAT_TIMER)
            if server.is_dead():
                continue
            server.net.inbox.put(HeartBeat())

    # A thread for each server and server pair to move the message from the outbox to
    # the corresponding inbox
    def message_mover(self, src, des):
        while True:
            if self._should_stop():
                return
            time.sleep(self.MESSAGE_DELAY)
            if src.is_dead() or des.is_dead():
                continue
            message = src.net.outbox[des.net.addr].get()
            des.net.inbox.put(message)

    def run(self):
        # start monitor
        Thread(target=self.monitor).start()

        # start all server's event driven run function
        for server in self.servers:
            Thread(target=server.event_driven_run).start()

        # start the election timers
        for server in self.servers:
            Thread(target=self.election_timer, args=[server]).start()

        # start the heartbeat timers
        for server in self.servers:
            Thread(target=self.heartbeat_timer, args=[server]).start()

        # start all message movers
        for src, des in permutations(self.servers, 2):
            Thread(target=self.message_mover, args=[src, des]).start()

        # start client
        Thread(target=self.client).start()


def basic_test():
    sim = EventDrivenSimulator(size=3, run_time=300)
    sim.run()


def kill_leader():
    sim = EventDrivenSimulator(size=3, run_time=300)
    sim.run()
    Thread(target=sim.mess_with_leader).start()


if __name__ == "__main__":
    logging.basicConfig(
        filename="test.log",
        level=logging.INFO,
        format="%(relativeCreated)6d %(threadName)s %(message)s",
    )
    # basic_test()
    kill_leader()
