from dataclasses import dataclass
import datetime
from typing import List, Optional, Set


from jql.types import Fact


@dataclass()
class Change:
    ref: Optional[Fact]
    facts: Set[Fact]
    revoke: bool = False


@dataclass()
class ChangeSet:
    client: str
    created: datetime.datetime
    query: str
    changes: List[Change]
