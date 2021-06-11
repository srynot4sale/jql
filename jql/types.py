from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Literal, Optional, Union, Set


@dataclass(frozen=True)
class Fact:
    tag: str
    fact: str
    value: str

    def is_tag(self) -> bool:
        return not self.fact and not self.value

    def is_fact(self) -> bool:
        return not self.is_tag()

    def is_ref(self) -> bool:
        return self.fact == "id"

    def is_primary_ref(self) -> bool:
        return self.is_ref() and self.tag == "db"

    def is_content(self) -> bool:
        return self.tag == "db" and self.fact == "content"

    def has_value(self) -> bool:
        return len(self.value) > 0

    def __str__(self) -> str:
        if self.is_tag():
            return f"#{self.tag}"
        else:
            if self.is_content():
                return self.value
            elif self.is_primary_ref():
                return f"@{self.value}"
            elif not self.has_value():
                return f'#{self.tag}/{self.fact}'
            else:
                return f'#{self.tag}/{self.fact}={self.value}'


def Tag(tag: str) -> Fact:
    return Fact(tag=tag, fact="", value="")


def FactFlag(tag: str, fact: str) -> Fact:
    return Fact(tag=tag, fact=fact, value="")


def FactValue(tag: str, fact: str, value: str) -> Fact:
    return Fact(tag=tag, fact=fact, value=value)


def FactRef(tag: str, ref: str) -> Fact:
    return Fact(tag=tag, fact="id", value=ref)


def Ref(ref: str) -> Fact:
    return FactRef("db", ref)


def Content(value: str) -> Fact:
    return FactValue(tag="db", fact="content", value=value)


ItemDict = Dict[str, Dict[str, Union[str, Literal[True]]]]


@dataclass(frozen=True)
class Item:
    """
    An item is a group of facts at a point in time
    """
    facts: Set[Fact]

    @property
    def ref(self) -> Optional[Ref]:
        for i in self.facts:
            if i.is_primary_ref():
                return Ref(i.value)
        return None

    @property
    def content(self) -> str:
        for i in self.facts:
            if i.is_content():
                return str(i)
        return ""

    def __str__(self) -> str:
        content = self.content
        facts = []
        tags = []

        for t in self.tags():
            tags.append(str(t))
        for f in self.not_tags():
            facts.append(str(f))

        data = tags + facts
        if content:
            data.insert(0, content)

        return f"@{self.ref} {' '.join(data)}"

    def as_dict(self) -> ItemDict:
        i: ItemDict = {}
        for t in self.tags():
            i[t] = {}
        for f in self.not_tags():
            i[f.tag][f.fact] = f.value if f.has_value() else True
        return i

    def tags(self) -> Set[str]:
        return {f.tag for f in self.facts if f.is_tag() and f.tag != "db"}

    def not_tags(self) -> Set[Fact]:
        return {f for f in self.facts if f.is_fact() and f.tag != "db"}

    def add_facts(self, add_facts: Set[Fact]) -> Item:
        return Item(self.facts.union(add_facts))
