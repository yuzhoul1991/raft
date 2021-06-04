import attr
import rpc
import network
import random
import log as raft_log
import logging

from typing import List
from enum import Enum


@attr.s
class Timeout:
    peers = attr.ib()
    election_timeout = attr.ib(init=False)
    heartbeat_timeout = attr.ib(init=False)

    def __attrs_post_init__(self):
        self.election_timeout = random.randint(500, 1000)
        self.heartbeat_timeout = random.randint(100, 200)

    def reset_election_timeout(self):
        self.election_timeout = random.randint(500, 1000)

    def reset_peer_heartbeat_timeout(self):
        self.heartbeat_timeout = random.randint(100, 200)

    # returns True if the timer has reached 0
    def decrement_election_timeout(self) -> bool:
        self.election_timeout -= 1
        if self.election_timeout == 0:
            self.reset_election_timeout()
            return True
        return False

    # returns True if the timer has reached 0
    def decrement_heartbeat_timeout(self) -> bool:
        self.heartbeat_timeout -= 1
        if self.heartbeat_timeout == 0:
            self.reset_peer_heartbeat_timeout()
            return True
        return False


class State(Enum):
    Leader = 1
    Follower = 2
    Candidate = 3
    Dead = 4


@attr.s(repr=False)
class RaftServer:
    net = attr.ib()
    current_term = attr.ib(default=0)
    log = attr.ib(default=attr.Factory(raft_log.RaftLog))
    voted_for = attr.ib(default=None)

    state = attr.ib(default=State.Follower)
    commit_index = attr.ib(default=-1)
    last_applied = attr.ib(default=-1)
    id = attr.ib(init=False)
    # for each follower, index of the next log entry to send to that follower
    next_index = attr.ib(init=False)
    # for each follower, index of highest entry known to be replicated on that follower
    match_index = attr.ib(init=False)

    # timeouts
    timeout = attr.ib(init=False)

    # this is only used in even driven simulator
    election_resetted = attr.ib(default=False)

    # the number of votes received, this doesn't count the self vote
    votes = attr.ib(default=attr.Factory(set))

    def __attrs_post_init__(self):
        # use the network address as id
        self.id = self.net.addr
        self.next_index = {peer: len(self.log) for peer in self.net.peers}
        self.match_index = {peer: -1 for peer in self.net.peers}
        self.timeout = Timeout(peers=self.net.peers)

    def __repr__(self):
        return f"Server:{self.id}, {self.state}, Term:{self.current_term}, voted_for={self.voted_for}, election_resetted={self.election_resetted}, votes={self.votes}"

    def _build_append_entries_response(self, success, match_index=0, debug_msg=""):
        return rpc.AppendEntriesResponse(
            src=self.id,
            success=success,
            current_term=self.current_term,
            debug_msg=debug_msg,
            match_index=match_index,
        )

    def _has_majority_votes(self):
        cluster_size = len(self.net.outbox)
        return len(self.votes) + 1 > cluster_size / 2

    def _build_append_entries_request(self, prev_entry_index, prev_entry_term, entries):
        return rpc.AppendEntriesRequest(
            src=self.id,
            current_term=self.current_term,
            prev_entry_index=prev_entry_index,
            prev_entry_term=prev_entry_term,
            entries=entries,
            leader_commit_index=self.commit_index,
        )

    # handle AppendEntriesRequest from a leader as a follower
    def follower_append_entries(
        self, request: rpc.AppendEntriesRequest
    ) -> rpc.AppendEntriesResponse:
        if not request.is_valid():
            return self._build_append_entries_response(
                success=False, debug_msg=f"Invalid AppendEntriesRequest: {request}"
            )

        # If the leader's term is less than the follower's term, then the request is invalid
        if request.current_term < self.current_term:
            return self._build_append_entries_response(
                success=False,
                debug_msg=f"Leader:{request.src}'s term:{request.current_term} is less than follower:{self.id}'s term:{self.current_term}",
            )

        before_log = list(self.log)
        success = self.log.append_entries(
            request.prev_entry_index, request.prev_entry_term, request.entries
        )
        if not success:
            assert before_log == list(self.log)
            return self._build_append_entries_response(
                success=False,
                debug_msg=f"Follower:{self.id} rejected append entry to its log",
            )

        # Note match_index is not the current length of the follower's log
        return self._build_append_entries_response(
            success=True, match_index=request.prev_entry_index + len(request.entries)
        )

    def _get_term(self, prev_index):
        if prev_index == -1:
            return 0
        else:
            return self.log[prev_index].term

    def _leader_send_append_entries_request(self, peer, empty_request=False):
        prev_index = self.next_index[peer] - 1
        entries = self.log[self.next_index[peer] :]
        if empty_request:
            entries = []
        request = self._build_append_entries_request(
            prev_index, self._get_term(prev_index), entries
        )

        self.net.send(peer, request)

    def send_heart_beat(self):
        for peer in self.net.peers:
            self._leader_send_append_entries_request(peer, empty_request=True)

    # When a client send a request and leader has a new entry to append to it's own log and
    # try to replicate to all followers
    def leader_append_entries(self, entries: List[raft_log.LogEntry]):

        # append the entry to leader's own log
        prev_entry_index = len(self.log) - 1

        success = self.log.append_entries(
            prev_entry_index, self._get_term(prev_entry_index), entries
        )

        if success:
            # broadcase this to all followers using what leader thinks the follower's next log entry
            # should be
            for peer in self.net.peers:
                self._leader_send_append_entries_request(peer)

    def follower_handle_append_entries_request(self, request: rpc.AppendEntriesRequest):
        response = self.follower_append_entries(request)
        # If successful follower can commit up to request.leader_commit
        self.commit_index = min(request.leader_commit_index, len(self.log) - 1)
        self.net.send(request.src, response)

        # reset election timer
        self.election_resetted = True
        self.timeout.reset_election_timeout()

        # The follower can now apply the commited entries
        if self.commit_index > self.last_applied:
            logging.info(
                f"Follower:{self.id} applying: {self.log[self.last_applied+1:self.commit_index+1]}"
            )
            self.last_applied = self.commit_index

    def leader_handle_append_entires_response(
        self, response: rpc.AppendEntriesResponse
    ):
        assert response.src in self.net.peers

        # if successful update some book-keeping stuff for this follower
        peer = response.src
        if response.success:
            self.match_index[peer] = response.match_index
            self.next_index[peer] = self.match_index[peer] + 1
            # if successful, we can check consensus and apply.
            # consensus means the entry has been replicated to the majority of the followers
            # indicated by the median of the match index from all followers
            # If consensus reached on some entry that has yet to be commited, mark as commit and apply
            highest_replicated = sorted(self.match_index.values())[
                len(self.net.peers) // 2
            ]
            if highest_replicated > self.commit_index:
                self.commit_index = highest_replicated
                logging.info(
                    f"Leader:{self.id} applying: {self.log[self.last_applied+1:self.commit_index+1]}"
                )
                self.last_applied = self.commit_index

        else:
            # if wasn't successful, decrement the follower's next_index and try again
            self.next_index[peer] = max(0, self.next_index[peer] - 1)
            self._leader_send_append_entries_request(response.src)

    def _build_request_vote_response(self, granted, debug_msg=""):
        return rpc.RequestVoteResponse(
            src=self.id,
            current_term=self.current_term,
            vote_granted=granted,
            debug_msg=debug_msg,
        )

    def server_vote(self, request: rpc.RequestVoteRequest) -> rpc.RequestVoteResponse:
        if request.current_term < self.current_term:
            return self._build_request_vote_response(
                False,
                f"Candidate:{request.src} term:{request.current_term} < {self.id}'s term: {self.current_term}",
            )

        if self.voted_for is not None and self.voted_for != request.src:
            return self._build_request_vote_response(
                False, f"{self.id} already voted for {self.voted_for}"
            )

        def last_term(log):
            return 0 if len(log) == 0 else log[-1].term

        def my_log_more_up_to_date():
            if last_term(self.log) > request.last_log_term:
                return True
            elif last_term(self.log) == request.last_log_term:
                return len(self.log) - 1 > request.last_log_index
            else:
                return False

        if my_log_more_up_to_date():
            return self._build_request_vote_response(
                False, f"{self.id} has more updated log"
            )
        self.voted_for = request.src
        return self._build_request_vote_response(True)

    def handle_request_vote_request(self, request: rpc.RequestVoteRequest):
        response = self.server_vote(request)
        self.net.send(request.src, response)

    def handle_request_vote_response(self, response: rpc.RequestVoteResponse):
        # discard the response if not in candidate state
        if self.state != State.Candidate:
            return
        if response.vote_granted:
            self.votes.add(response.src)
            if self._has_majority_votes():
                self.update_state(State.Leader)

    def _start_election(self):
        # TODO: reset election timer
        for peer in self.net.peers:
            vote_request = rpc.RequestVoteRequest(
                src=self.id,
                current_term=self.current_term,
                last_log_index=-1 if len(self.log) == 0 else len(self.log) - 1,
                last_log_term=0 if len(self.log) == 0 else self.log[-1].term,
            )
            self.net.send(peer, vote_request)

    def _update_term(self, new_term):
        # need to reset voted_for when increment term
        self.voted_for = None
        self.current_term = new_term

    def update_state(self, new_state):
        self.state = new_state
        if new_state == State.Candidate:
            self._update_term(self.current_term + 1)
            self.voted_for = self.id
            self._start_election()
            self.timeout.reset_election_timeout()
        elif new_state == State.Leader:
            # once become leader, assert authority by sending heartbeat to
            # all peers
            self.send_heart_beat()

    def _handle_incoming_message(self, message):
        assert message.des == self.id
        packet = message.data
        if packet:
            # if any Rpc contains a current_term > self.current_term, update to term and convert to
            # Follower state
            if packet.current_term > self.current_term:
                self._update_term(packet.current_term)
                self.update_state(State.Follower)

            # If a response from other server contains stale term, discard it
            def is_stale(response):
                return response.current_term < self.current_term

            if isinstance(packet, rpc.AppendEntriesRequest):
                # If still in candidate state, revert to follower
                # This isn't covered by checking the term, they might have the same term in
                # a contested voting case. ie. both were candidates
                if self.state == State.Candidate:
                    self.update_state(State.Follower)
                # logging.debug(f"Server:{self.id} term:{self.current_term} received: {packet}")
                self.follower_handle_append_entries_request(packet)

            elif isinstance(packet, rpc.AppendEntriesResponse):
                if is_stale(packet):
                    return
                # A follower can also receive an AppendEntriesResponse if it was the leader before
                # but got converted to a follower, but it's previous responsese just arrived
                # logging.debug(f"Server:{self.id} term:{self.current_term} received: {packet}")
                self.leader_handle_append_entires_response(packet)

            elif isinstance(packet, rpc.RequestVoteRequest):
                # TODO: what self.state is expected here?
                # logging.debug(f"Server:{self.id} term:{self.current_term} received: {packet}")
                self.handle_request_vote_request(packet)

            elif isinstance(packet, rpc.RequestVoteResponse):
                # logging.debug(f"Server:{self.id} term:{self.current_term} received: {packet}")
                if is_stale(packet):
                    return
                self.handle_request_vote_response(packet)

            else:
                raise ValueError("Not expecting message: ", message)
        else:
            logging.critical(f"Empty rpc message: {message}")

    def handle_incoming_messages(self):
        if self.state == State.Dead:
            return
        while not self.net.inbox.empty():
            self._handle_incoming_message(self.net.receive())

    def is_dead(self):
        return self.state == State.Dead

    # server thread used by the event driven simulator
    def event_driven_run(self):
        while True:
            packet = self.net.inbox.get()
            logging.debug("\n")
            logging.debug(self)
            logging.debug(packet)
            if isinstance(packet, rpc.Election):
                if self.election_resetted:
                    logging.debug(
                        f"Server{self.id}: Ignored Election event due to election_resetted"
                    )
                    self.election_resetted = False
                    continue
                if not self.state in [State.Follower, State.Candidate]:
                    logging.debug(
                        f"Server{self.id}: Ignored Election event due to state"
                    )
                    self.election_resetted = False
                    continue
                self.update_state(State.Candidate)
                self.election_resetted = False

            elif isinstance(packet, rpc.HeartBeat):
                # ignore this if not in leader state
                if self.state != State.Leader:
                    logging.debug(
                        f"Server{self.id}: Ignored HeartBeat event since not leader"
                    )
                    continue
                self.send_heart_beat()

            else:
                self._handle_incoming_message(packet)


def basic_test():
    leader = RaftServer(
        net=network.RaftNet(
            addr=0,
            peers=[1],
        ),
        state=State.Leader,
    )
    follower = RaftServer(
        net=network.RaftNet(
            addr=1,
            peers=[0],
        )
    )

    leader.leader_append_entries(
        [raft_log.LogEntry(1), raft_log.LogEntry(1), raft_log.LogEntry(2)]
    )

    # check the leader has populated the outbox
    assert leader.net.outbox[1].qsize() == 1

    simulate([leader, follower])
    assert leader.log == follower.log


# configure one leader and see if it can replicate log to all other followers
# leader and follower all starts with empty
def cluster_test():
    nets = create_cluster(5)
    servers = [RaftServer(net) for net in nets]
    leader, followers = servers[0], servers[1:]
    leader.state = State.Leader

    leader.leader_append_entries(
        [raft_log.LogEntry(1), raft_log.LogEntry(1), raft_log.LogEntry(2)]
    )

    simulate(servers)

    # handle messages for all followers
    for follower in followers:
        follower.handle_incoming_messages()
        assert follower.log == leader.log


def figure7_test():
    nets = create_cluster(7)
    leader = RaftServer(
        net=nets[0],
        log=raft_log.RaftLog(
            log=[
                raft_log.LogEntry(1),
                raft_log.LogEntry(1),
                raft_log.LogEntry(1),
                raft_log.LogEntry(4),
                raft_log.LogEntry(4),
                raft_log.LogEntry(5),
                raft_log.LogEntry(5),
                raft_log.LogEntry(6),
                raft_log.LogEntry(6),
                raft_log.LogEntry(6),
            ]
        ),
        state=State.Leader,
    )

    follower_a = RaftServer(
        net=nets[1],
        log=raft_log.RaftLog(
            log=[
                raft_log.LogEntry(1),
                raft_log.LogEntry(1),
                raft_log.LogEntry(1),
                raft_log.LogEntry(4),
                raft_log.LogEntry(4),
                raft_log.LogEntry(5),
                raft_log.LogEntry(5),
                raft_log.LogEntry(6),
                raft_log.LogEntry(6),
            ]
        ),
    )

    follower_b = RaftServer(
        net=nets[2],
        log=raft_log.RaftLog(
            log=[
                raft_log.LogEntry(1),
                raft_log.LogEntry(1),
                raft_log.LogEntry(1),
                raft_log.LogEntry(4),
            ]
        ),
    )

    follower_c = RaftServer(
        net=nets[3], log=raft_log.RaftLog(log=leader.log.log + [raft_log.LogEntry(6)])
    )

    follower_d = RaftServer(
        net=nets[4],
        log=raft_log.RaftLog(
            log=leader.log.log + [raft_log.LogEntry(7), raft_log.LogEntry(7)]
        ),
    )

    follower_e = RaftServer(
        net=nets[5],
        log=raft_log.RaftLog(
            log=[
                raft_log.LogEntry(1),
                raft_log.LogEntry(1),
                raft_log.LogEntry(1),
                raft_log.LogEntry(4),
                raft_log.LogEntry(4),
                raft_log.LogEntry(4),
                raft_log.LogEntry(4),
            ]
        ),
    )

    follower_f = RaftServer(
        net=nets[6],
        log=raft_log.RaftLog(
            log=[
                raft_log.LogEntry(1),
                raft_log.LogEntry(1),
                raft_log.LogEntry(1),
                raft_log.LogEntry(2),
                raft_log.LogEntry(2),
                raft_log.LogEntry(2),
                raft_log.LogEntry(3),
                raft_log.LogEntry(3),
                raft_log.LogEntry(3),
                raft_log.LogEntry(3),
                raft_log.LogEntry(3),
            ]
        ),
    )

    servers = [
        leader,
        follower_a,
        follower_b,
        follower_c,
        follower_d,
        follower_e,
        follower_f,
    ]

    leader.current_term = 8
    leader.leader_append_entries([raft_log.LogEntry(8)])

    simulate(servers)

    def verify_result(leader, peers):
        for peer in peers:
            print(f"Checking Follower:{peer.id}")
            if leader.log != peer.log:
                import pdb

                pdb.set_trace()
            assert leader.log == peer.log
            assert leader.next_index[peer.id] == len(leader.log)

    verify_result(servers[0], servers[1:])


def test_election():
    nets = create_cluster(3)
    servers = [RaftServer(net=net, log=raft_log.RaftLog()) for net in nets]
    candidate = servers[0]
    candidate.update_state(State.Candidate)

    another_candidate = servers[1]
    another_candidate.update_state(State.Candidate)

    simulate(servers)

    # since delivery of message is random, any one of the two could become
    # the leader
    assert not all(
        server.state == State.Leader for server in [candidate, another_candidate]
    )


if __name__ == "__main__":
    from simulator import *

    # basic_test()
    # cluster_test()
    for _ in range(100):
        figure7_test()
        test_election()
