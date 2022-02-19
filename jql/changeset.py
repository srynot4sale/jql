from dataclasses import dataclass
import datetime
from typing import Any, Dict, List, Optional, Set


from jql.types import Fact, fact_from_dict, Ref


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

    @classmethod
    def from_dict(cls, c: Dict[str, Any]) -> 'Change':
        if c.get('ref'):
            ref = Ref(c['ref'])
        else:
            ref = None

        return Change(
            facts={fact_from_dict(f) for f in c.get('facts', [])},
            ref=ref,
            uid=c.get('uid', None),
            revoke=c.get('revoke', False)
        )


@dataclass()
class ChangeSet:
    uuid: str
    client: str
    created: datetime.datetime
    query: str
    changes: List[Change]

    def changes_as_dict(self) -> List[Dict[str, Any]]:
        return [c.to_dict() for c in self.changes]

    @classmethod
    def changes_from_dict(cls, changes: List[Dict[str, Any]]) -> List[Change]:
        return [Change.from_dict(c) for c in changes]
