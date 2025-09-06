from enum import Enum
from functools import total_ordering

@total_ordering
class Policy(Enum):
    KEEP = "keep"
    PROMPT = "prompt"
    HARDLINK = "hardlink"
    TRASH = "trash"
    DELETE = "delete"
    DISCARD = "discard!"
    ERASE = "erase!"
    NOACTION = "noaction"

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Policy):
            return NotImplemented
        return policy_priority(self) < policy_priority(other)

_POLICY_PRIORITY: dict[Policy, int] = {
    Policy.KEEP: 100,
    Policy.PROMPT: 75,
    Policy.HARDLINK: 50,
    Policy.TRASH: 40,
    Policy.DELETE: 30,
    Policy.DISCARD: 20,
    Policy.ERASE: 10,
    Policy.NOACTION: 0
}

def policy_priority(policy: str|Policy|int) -> int:
    if isinstance(policy, str):
        if policy not in Policy._value2member_map_:
            raise ValueError(f"Unknown directory policy: {policy}")
        policy = Policy(policy)
    if isinstance(policy, int):
        if policy not in _POLICY_PRIORITY.values():
            raise ValueError(f"Unknown directory policy priority: {policy}")
        return policy
    return _POLICY_PRIORITY[policy]
