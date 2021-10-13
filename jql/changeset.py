from dataclasses import dataclass
import datetime
from typing import List, Optional, Set


from jql.types import Fact


@dataclass()
class Change:
    facts: Set[Fact]
    ref: Optional[Fact] = None
    uid: Optional[str] = None
    revoke: bool = False


@dataclass()
class ChangeSet:
    client: str
    created: datetime.datetime
    query: str
    changes: List[Change]
