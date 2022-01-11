from dataclasses import dataclass
import datetime
from typing import Any, Dict, List, Optional, Set


from jql.types import Fact


@dataclass()
class Change:
    facts: Set[Fact]
    ref: Optional[Fact] = None
    uid: Optional[str] = None
    revoke: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            'facts': [f._asdict() for f in self.facts],
            'ref': self.ref.value if self.ref else None,
            'uid': self.uid,
            'revoke': self.revoke
        }


@dataclass()
class ChangeSet:
    uuid: str
    client: str
    created: datetime.datetime
    query: str
    changes: List[Change]

    def changes_as_dict(self) -> List[Dict[str, Any]]:
        return [c.to_dict() for c in self.changes]
