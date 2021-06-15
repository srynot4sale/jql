from dataclasses import dataclass
from typing import Optional, Set


from jql.types import Item, Fact


@dataclass()
class Change:
    item: Optional[Item]
    facts: Set[Fact]
