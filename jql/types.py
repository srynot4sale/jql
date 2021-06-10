from __future__ import annotations
from dataclasses import dataclass, field
from typing import cast, Dict, Optional, Set, Type, TypeVar


@dataclass(frozen=True)
class Ref:
    ref: str

    def __str__(self) -> str:
        return f"@{self.ref}"


@dataclass(frozen=True)
class Prop:
    tag: str
    fact: Optional[str]
    value: Optional[str]

    def __init__(self) -> None:
        raise Exception("Abstract")


@dataclass(frozen=True)
class Tag(Prop):
    fact: None = field(init=False, default=None)
    value: None = field(init=False, default=None)

    def __str__(self) -> str:
        return f"#{self.tag}"


@dataclass(frozen=True)
class Fact(Prop):
    fact: str

    def __init__(self) -> None:
        raise Exception("Abstract")


@dataclass(frozen=True)
class FactFlag(Fact):
    value: None = field(init=False, default=None)

    def __str__(self) -> str:
        return f'#{self.tag}/{self.fact}'


@dataclass(frozen=True)
class FactValue(Fact):
    value: str

    def __str__(self) -> str:
        return f'#{self.tag}/{self.fact}={self.value}'


@dataclass(frozen=True)
class FactId(FactValue):
    fact: str = field(init=False, default="id")


@dataclass(frozen=True)
class Content(FactValue):
    tag: str = field(init=False, default="db")
    fact: str = field(init=False, default="content")

    def __str__(self) -> str:
        return self.value


T = TypeVar('T', bound=Prop)

ItemDict = Dict[str, Dict[str, Optional[str]]]


@dataclass(frozen=True)
class Item:
    """
    An item is a group of facts at a point in time
    """
    facts: Set[Prop]

    @property
    def ref(self) -> Optional[Ref]:
        for i in self.filter(FactId):
            if i.tag == "db":
                return Ref(i.value)
        return None

    def __str__(self) -> str:
        content = None
        facts = []
        tags = []

        for t in self.filter(Tag):
            if t.tag == "db":
                continue
            tags.append(str(t))
        for f in self.filter(Fact):
            if type(f) == Content:
                content = str(f)
            else:
                facts.append(str(f))

        data = tags + facts
        if content:
            data.insert(0, content)

        return f"@{self.ref} {' '.join(data)}"

    def as_dict(self) -> ItemDict:
        i: ItemDict = {}
        for t in self.tags():
            i[t] = {}
        for f in self.filter(Fact):
            i[f.tag][f.fact] = f.value if type(f) == FactValue else None
        return i

    def filter(self, ptype: Type[T]) -> Set[T]:
        return {cast(T, f) for f in self.facts if isinstance(f, ptype)}

    def tags(self) -> Set[str]:
        return {f.tag for f in self.filter(Tag)}

    def add_facts(self, add_facts: Set[Prop]) -> Item:
        return Item(self.facts.union(add_facts))
