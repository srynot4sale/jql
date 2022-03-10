from dataclasses import dataclass
import datetime
from typing import Any, Dict, List, Set


from jql.types import Fact, fact_from_dict


@dataclass()
class Change:
    facts: Set[Fact]
    uuid: str
    revoke: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            'facts': sorted([f._asdict() for f in self.facts], key=repr),
            'uuid': self.uuid,
            'revoke': self.revoke
        }

    @classmethod
    def from_dict(cls, c: Dict[str, Any]) -> 'Change':
        return Change(
            facts={fact_from_dict(f) for f in c.get('facts', [])},
            uuid=c['uuid'],
            revoke=bool(c.get('revoke', False))
        )


@dataclass()
class ChangeSet:
    uuid: str
    client: str
    origin: str
    origin_rowid: int
    created: datetime.datetime
    query: str
    changes: List[Change]
    applied: bool = False
    replicated: bool = False

    def changes_as_dict(self) -> List[Dict[str, Any]]:
        return sorted([c.to_dict() for c in self.changes], key=repr)

    @classmethod
    def changes_from_dict(cls, changes: List[Dict[str, Any]]) -> List[Change]:
        return [Change.from_dict(c) for c in changes]
