from dataclasses import dataclass

@dataclass
class FailureEvent:
    sn: str
    fail_index: int
    station: str
    error_code: str
    fail_rank: int


@dataclass
class RepairAttempt:
    technician_badge: str
    failure: FailureEvent
    swap_index: int
