from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Dict, Set, Iterable


@dataclass(frozen=True)
class Fact:
    tag: str
    prop: str
    value: str

    def __str__(self) -> str:
        if is_tag(self):
            return f"#{self.tag}"
        else:
            if is_content(self):
                return self.value
            elif is_primary_ref(self):
                return f"@{self.value}"
            elif not has_value(self):
                return f'#{self.tag}/{self.prop}'
            else:
                return f'#{self.tag}/{self.prop}={self.value}'

    def __repr__(self) -> str:
        if is_tag(self):
            return f"Tag('{self.tag}')"
        else:
            if is_content(self):
                return f"Content('{self.value}')"
            elif is_primary_ref(self):
                return f"Ref('{self.value}')"
            elif not has_value(self):
                return f"Flag(tag='{self.tag}', prop='{self.prop}')"
            else:
                return f"Value(tag='{self.tag}', prop='{self.prop}', value='{self.value}')"


def tag_eq(tag: str) -> Callable[[Fact], bool]:
    return lambda fact: fact.tag == tag


def prop_eq(prop: str) -> Callable[[Fact], bool]:
    return lambda fact: fact.prop == prop


def value_eq(value: str) -> Callable[[Fact], bool]:
    return lambda fact: fact.value == value


def has_prop(fact: Fact) -> bool:
    return not prop_eq("")(fact)


def has_value(fact: Fact) -> bool:
    return not value_eq("")(fact)


def has_sys_tag(fact: Fact) -> bool:
    return tag_eq("db")(fact)


def is_tag(fact: Fact) -> bool:
    return prop_eq("")(fact) and value_eq("")(fact)


def is_prop(fact: Fact) -> bool:
    return not prop_eq("")(fact)


def is_ref(fact: Fact) -> bool:
    return prop_eq("id")(fact)


def is_primary_ref(fact: Fact) -> bool:
    return is_ref(fact) and has_sys_tag(fact)


def is_content(fact: Fact) -> bool:
    return has_sys_tag(fact) and prop_eq("content")(fact)


def is_flag(fact: Fact) -> bool:
    return is_prop(fact) and not has_value(fact)


def Tag(tag: str) -> Fact:
    return Fact(tag=tag, prop="", value="")


def Flag(tag: str, prop: str) -> Fact:
    return Fact(tag=tag, prop=prop, value="")


def Value(tag: str, prop: str, value: str) -> Fact:
    return Fact(tag=tag, prop=prop, value=value)


def TagRef(tag: str, ref: str) -> Fact:
    return Fact(tag=tag, prop="id", value=ref)


def Ref(ref: str) -> Fact:
    return TagRef("db", ref)


def Content(value: str) -> Fact:
    return Value(tag="db", prop="content", value=value)


@dataclass(frozen=True)
class Item:
    """
    An item is a group of facts at a point in time
    """
    facts: frozenset[Fact]

    def __init__(self, facts: Iterable[Fact]):
        super().__setattr__("facts", frozenset(facts))

    @property
    def ref(self) -> str:
        for f in filter(is_primary_ref, self.facts):
            return f.value
        else:
            raise Exception("No ref")

    @property
    def content(self) -> str:
        return str(next(filter(is_content, self.facts), ""))

    def __str__(self) -> str:
        content = self.content
        props = [str(d) for d in get_props(self)]
        tags = [str(d) for d in get_tags(self)]
        if content:
            strs = [content] + tags + props

        return f"@{self.ref} {' '.join(strs)}"

    def as_dict(self) -> Dict[str, Dict[str, str]]:
        i: Dict[str, Dict[str, str]] = {}
        for t in get_tags(self):
            i[t.tag] = {}
        if self.content:
            i["db"] = {"content": self.content}
        for f in get_props(self):
            i[f.tag][f.prop] = f.value
        return i


def get_tags(item: Item) -> Set[Fact]:
    return {Tag(f.tag) for f in item.facts if not has_sys_tag(f)}


def get_props(item: Item) -> Set[Fact]:
    return {f for f in item.facts if is_prop(f) and not has_sys_tag(f)}


def get_flags(item: Item) -> Set[Fact]:
    return {Flag(f.tag, f.prop) for f in get_props(item)}


def update_item(item: Item, add: Iterable[Fact]) -> Item:
    return Item(item.facts.union(add))
