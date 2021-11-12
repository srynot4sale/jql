from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Set, Iterable, Generator


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

    def __len__(self) -> int:
        return len(self.tag) + len(self.prop) + len(self.value)

    def __iter__(self) -> Generator:  # type: ignore
        yield 'tag', self.tag
        if len(self.prop):
            yield 'prop', self.prop
        if len(self.value):
            yield 'value', self.value

    def as_tuple(self) -> tuple[str, str, str]:
        return (self.tag, self.prop, self.value)


def tag_eq(tag: str) -> Callable[[Fact], bool]:
    return lambda fact: require_fact(fact) and fact.tag == tag


def prop_eq(prop: str) -> Callable[[Fact], bool]:
    return lambda fact: require_fact(fact) and fact.prop == prop


def value_eq(value: str) -> Callable[[Fact], bool]:
    return lambda fact: require_fact(fact) and fact.value == value


def has_prop(fact: Fact) -> bool:
    return not prop_eq("")(fact)


def has_value(fact: Fact) -> bool:
    return not value_eq("")(fact)


def has_sys_tag(fact: Fact) -> bool:
    return tag_eq("db")(fact)


def require_fact(fact: Fact) -> bool:
    if not isinstance(fact, Fact):
        raise Exception(f"Expected a Fact, but received a {fact.__class__.__name__}")
    return True


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


def fact_from_dict(f: dict[str, str]) -> Fact:
    return Fact(tag=f['tag'], prop=f.get('prop', ''), value=f.get('value', ''))


@dataclass(frozen=True)
class Item:
    """
    An item is a group of facts at a point in time
    """
    facts: frozenset[Fact]

    def __init__(self, facts: Iterable[Fact]):
        super().__setattr__("facts", frozenset(facts))

    @property
    def ref(self) -> Fact:
        f = list(filter(is_primary_ref, self.facts))
        if len(f) == 1:
            return f[0]
        elif len(f) == 0:
            raise Exception("No ref")
        else:
            raise Exception("Multiple primary refs found")

    @property
    def content(self) -> Fact:
        c = list(filter(is_content, self.facts))
        if len(c) == 1:
            return c[0]
        elif len(c) == 0:
            return Content("")
        else:
            raise Exception("Multiple content facts found")

    def __str__(self) -> str:
        output: list[str] = []
        if has_ref(self):
            output.append(str(self.ref))

        if self.content:
            output.append(str(self.content))

        output.extend([str(d) for d in get_tags(self)])
        output.extend([str(d) for d in get_props(self)])

        return ' '.join(output)

    def as_tuples(self) -> set[tuple[str, str, str]]:
        return {f.as_tuple() for f in self.facts if not is_primary_ref(f)}

    @property
    def __iter__(self):  # type: ignore
        return self.facts.__iter__


def get_facts(item: Item) -> Set[Fact]:
    return {f for f in item.facts if not has_sys_tag(f)}


def get_tags(item: Item) -> Set[Fact]:
    return {Tag(f.tag) for f in get_facts(item)}


def get_props(item: Item) -> Set[Fact]:
    return {f for f in get_facts(item) if is_prop(f)}


def get_flags(item: Item) -> Set[Fact]:
    return {Flag(f.tag, f.prop) for f in get_props(item)}


def get_value(item: Item, tag: str, prop: str) -> str:
    return single((f for f in item.facts if tag_eq(tag)(f) and prop_eq(prop)(f) and has_value(f))).value


def update_item(item: Item, add: Iterable[Fact]) -> Item:
    return Item(item.facts.union(add))


def has_ref(item: Item) -> bool:
    return next(filter(is_primary_ref, item.facts), None) is not None


def single(facts: Iterable[Fact]) -> Fact:
    f = list(facts)
    if len(f) != 1:
        raise Exception(f'Expected a single fact, but got {len(f)}')
    return f[0]
