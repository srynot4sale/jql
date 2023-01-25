from __future__ import annotations
from typing import Any, Callable, Iterable, Iterator, NamedTuple, Optional


class ItemException(Exception):
    def __init__(self, message: str, item: Facts) -> None:
        self.item = item
        self.message = f'{message} ({repr(item)})'


class Fact(NamedTuple):
    tag: str
    prop: str
    value: str
    tx: Optional[str] = None

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
                return f'#{self.tag}/{self.prop}={value_wrap(self.value)}'

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

    def __lt__(self, other: Any) -> bool:
        return str(self) < str(other)

    def as_tuple(self) -> tuple[str, str, str]:
        return (self.tag, self.prop, self.value)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.as_tuple() == other.as_tuple()

    def __hash__(self) -> int:
        return hash(self.as_tuple())


def value_wrap(value: str) -> str:
    if not isinstance(value, str) or len(value.strip()) == 0:
        return ""
    elif value.isalpha():
        return value
    else:
        return f'[[[ {value} ]]]'


Facts = Iterable[Fact]


def tag_eq(tag: str) -> Callable[[Fact], bool]:
    "Return a function that checks the tag equals the supplied value"
    return lambda fact: require_fact(fact) and fact.tag == tag


def prop_eq(prop: str) -> Callable[[Fact], bool]:
    "Return a function that checks the prop equals the supplied value"
    return lambda fact: require_fact(fact) and fact.prop == prop


def value_eq(value: str) -> Callable[[Fact], bool]:
    "Return a function that checks the value equals the supplied value"
    return lambda fact: require_fact(fact) and fact.value == value


def has_prop(fact: Fact) -> bool:
    "Checks the fact has a prop set"
    return not prop_eq("")(fact)


def has_value(fact: Fact) -> bool:
    "Checks the fact has a value set"
    return not value_eq("")(fact)


def is_hidden_sys(fact: Fact) -> bool:
    return has_sys_tag(fact) and (is_tag(fact) or is_ref(fact) or prop_eq("content")(fact))


def has_sys_tag(fact: Fact) -> bool:
    return require_fact(fact) and fact.tag.startswith('_')


def require_fact(fact: Fact) -> bool:
    "Raise exception if param is not an instance of Fact"
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


def is_created(fact: Fact) -> bool:
    return has_sys_tag(fact) and prop_eq("created")(fact)


def is_flag(fact: Fact) -> bool:
    return is_prop(fact) and not has_value(fact)


def Tag(tag: str) -> Fact:
    if tag.startswith('#'):
        raise Exception('Strip off the # when using the Tag() function')
    return Fact(tag=tag, prop="", value="")


def Flag(tag: str, prop: str) -> Fact:
    return Fact(tag=tag, prop=prop, value="")


def Value(tag: str, prop: str, value: str) -> Fact:
    if not len(value):
        raise Exception("Empty value supplied")
    return Fact(tag=tag, prop=prop, value=value)


def TagRef(tag: str, ref: str) -> Fact:
    return Fact(tag=tag, prop="id", value=ref)


def Ref(ref: str) -> Fact:
    return TagRef("_db", ref)


def Content(value: str) -> Fact:
    if len(value):
        return Value(tag="_db", prop="content", value=value)
    else:
        return Flag(tag="_db", prop="content")


def fact_from_dict(f: dict[str, str]) -> Fact:
    return Fact(tag=f['tag'], prop=f.get('prop', ''), value=f.get('value', ''))


class Item:
    """
    An item is a group of facts at a point in time
    """
    facts: frozenset[Fact]

    def __init__(self, facts: Iterable[Fact]):
        super().__setattr__("facts", frozenset(facts))

    def __str__(self) -> str:
        output: list[str] = []
        if has_ref(self):
            output.append(str(get_ref(self)))

        content = get_content(self)
        if content and len(content):
            output.append(str(content))

        output.extend([str(d) for d in get_tags(self)])
        output.extend([str(d) for d in get_props(self)])

        return ' '.join(output)

    def __repr__(self) -> str:
        facts = ', '.join(sorted([repr(f) for f in self.facts]))
        return f'Item(facts={{{facts}}}'

    def as_tuples(self) -> set[tuple[str, str, str]]:
        props_tags = {Tag(f.tag) for f in get_props(self)}
        return {f.as_tuple() for f in self.facts if not is_primary_ref(f) and not is_created(f) and f not in props_tags}

    def __iter__(self) -> Iterator[Fact]:
        return self.facts.__iter__()

    def __next__(self) -> Fact:
        return next(self)


def get_facts(item: Facts) -> Facts:
    return {f for f in item if not is_hidden_sys(f)}


def get_all_tags(item: Facts) -> Facts:
    return {Tag(f.tag) for f in item}


def get_tags(item: Facts) -> Facts:
    return {t for t in get_all_tags(item) if not has_sys_tag(t)}


def get_props(item: Facts) -> Facts:
    return {f for f in item if not is_hidden_sys(f) and is_prop(f)}


def get_flags(item: Facts, for_tag: str = '') -> Facts:
    """
    Return all non-hidden flags for an item, optionally filtered by tag
    """
    flags = {Flag(f.tag, f.prop) for f in get_props(item)}
    if len(for_tag):
        flags = {f for f in flags if f.tag == for_tag}
    return flags


def get_fact(item: Facts, tag: str, prop: str) -> Fact:
    return single((f for f in item if tag_eq(tag)(f) and prop_eq(prop)(f)))


def get_value(item: Facts, tag: str, prop: str) -> str:
    fact = get_fact(item, tag, prop)
    if not has_value(fact):
        raise Exception(f'Expected a value, but got a flag: {fact}')
    return fact.value


def has_flag(item: Facts, tag: str, prop: str) -> bool:
    return len({f for f in item if tag_eq(tag)(f) and prop_eq(prop)(f)}) >= 1


def has_tag(item: Facts, tag: str) -> bool:
    return tag in {f.tag for f in item}


def update_item(item: Item, add: Iterable[Fact]) -> Item:
    return Item(item.facts.union(add))


def revoke_item_facts(item: Item, revoke: Iterable[Fact]) -> Item:
    return Item(item.facts.difference(revoke))


def has_ref(item: Item) -> bool:
    return next(filter(is_primary_ref, item.facts), None) is not None


def single(facts: Iterable[Fact]) -> Fact:
    f = list(facts)
    if len(f) != 1:
        raise Exception(f'Expected a single fact, but got {len(f)}: {list(facts)}')
    return f[0]


def is_tx(item: Facts) -> bool:
    return has_tag(item, "_tx")


def is_archived(item: Facts) -> bool:
    return has_flag(item, "_db", "archived")


def get_ref(item: Facts) -> Fact:
    f = list(filter(is_primary_ref, item))
    if len(f) == 1:
        return f[0]
    elif len(f) == 0:
        raise ItemException("No ref", item)
    else:
        raise ItemException("Multiple primary refs found", item)


def get_content(item: Facts) -> Fact:
    c = list(filter(is_content, item))
    if len(c) == 1:
        return c[0]
    elif len(c) == 0:
        return Content("")
    else:
        raise ItemException("Multiple content facts found", item)


def get_created_time(item: Facts) -> Fact:
    return get_fact(item, "_db", "created")
