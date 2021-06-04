import attr


class Event:
    pass


class HeartBeat(Event):
    pass


class Election(Event):
    pass


@attr.s
class RpcMessage(Event):
    # this is the term number that is included in every
    # rpc so that the sender of the request can update
    # themself
    current_term = attr.ib()
    src = attr.ib()
    des = attr.ib(init=False)


@attr.s
class AppendEntriesRequest(RpcMessage):
    prev_entry_index = attr.ib(default=-1)
    prev_entry_term = attr.ib(default=0)
    entries = attr.ib(default=[])
    leader_commit_index = attr.ib(default=-1)

    def is_valid(self) -> bool:
        return (
            self.current_term >= 0
            and self.prev_entry_index >= -1
            and self.prev_entry_term >= 0
            and self.leader_commit_index >= -1
        )


@attr.s
class AppendEntriesResponse(RpcMessage):
    success = attr.ib()
    match_index = attr.ib()
    debug_msg = attr.ib(default="")


@attr.s
class RequestVoteRequest(RpcMessage):
    last_log_index = attr.ib()
    last_log_term = attr.ib()


@attr.s
class RequestVoteResponse(RpcMessage):
    vote_granted = attr.ib(default=False)
    debug_msg = attr.ib(default="")
