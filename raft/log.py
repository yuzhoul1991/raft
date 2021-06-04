import attr


@attr.s
class LogEntry:
    term = attr.ib()
    command = attr.ib(default=None)


@attr.s
class RaftLog:
    log = attr.ib(default=attr.Factory(list))

    def __len__(self):
        return len(self.log)

    def __getitem__(self, index):
        return self.log[index]

    def append_entries(self, prev_entry_index, prev_entry_term, entries) -> bool:
        # The leader intends to replicate entries starting from prev_entry_index + 1

        # Consistency check
        # No gap is allowed in the follower's log
        if prev_entry_index >= len(self.log):
            return False

        # If follower doesn't have an entry described by (prev_entry_index, prev_entry_term), return False
        # No need to check len(self.log) as previous if covers that
        if prev_entry_index >= 0 and self.log[prev_entry_index].term != prev_entry_term:
            return False

        # If there is a conflicting new entry with existing, delete existing entry and all that follows
        if entries:
            for idx, entry in enumerate(entries):
                # If follower log has this entry but with a different term
                if (
                    prev_entry_index + 1 + idx < len(self.log)
                    and entry.term != self.log[prev_entry_index + 1 + idx].term
                ):
                    del self.log[prev_entry_index + 1 + idx :]
                    break

        # Replicate leader's entries
        # Note: cannot blinkdly do [prev_entry_index+1:] because there might be out-of-order messages
        # and that will delete valid entries in follower log, resulting in later unable to commit.
        self.log[prev_entry_index + 1 : prev_entry_index + 1 + len(entries)] = entries
        return True


def test_raftlog():
    def build_follower():
        follower = RaftLog()
        follower.log = [
            LogEntry(1),
            LogEntry(1),
            LogEntry(2),
        ]
        return follower

    # leader replicating first entry with higher term and follower has entries from previous terms
    follower = build_follower()
    assert follower.append_entries(-1, 0, [LogEntry(4)])
    assert len(follower) == 1

    # leader replicating first entry and follower log is empty
    follower = RaftLog()
    assert follower.append_entries(-1, 0, [LogEntry(1)])

    # follower with missing entries replicated
    follower = build_follower()
    assert follower.append_entries(2, 2, [LogEntry(3), LogEntry(3)])
    assert len(follower) == 5

    # follower has extra uncommited entries, but previous entries are consistent with leader's
    # follower should replicate leader's entries
    follower = build_follower()
    assert follower.append_entries(1, 1, [LogEntry(3), LogEntry(3)])
    assert len(follower) == 4

    # follower has extra uncommited entries, previous entries are inconsistent with leader's
    # should return False but leave follower log unchanged
    follower = build_follower()
    assert not follower.append_entries(1, 2, [LogEntry(3), LogEntry(3)])
    assert len(follower) == 3

    # leader tries to replicate entires that follow already has, should not truncate follower's
    # log
    follower = build_follower()
    assert follower.append_entries(0, 1, [LogEntry(1), LogEntry(2)])
    assert len(follower) == 3


if __name__ == "__main__":
    test_raftlog()
