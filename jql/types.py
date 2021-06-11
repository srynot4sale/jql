from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Dict, Set


@dataclass(frozen=True)
class Prop:
    tag: str
    fact: str
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
                return f'#{self.tag}/{self.fact}'
            else:
                return f'#{self.tag}/{self.fact}={self.value}'


def tag_eq(tag: str) -> Callable[[Prop], bool]:
    return lambda prop: prop.tag == tag


def fact_eq(fact: str) -> Callable[[Prop], bool]:
    return lambda prop: prop.fact == fact


def value_eq(value: str) -> Callable[[Prop], bool]:
    return lambda prop: prop.value == value


has_sys_tag = tag_eq("db")


def is_tag(prop: Prop) -> bool:
    return fact_eq("")(prop) and value_eq("")(prop)


def is_fact(prop: Prop) -> bool:
    return not is_tag(prop)


def is_ref(prop: Prop) -> bool:
    return fact_eq("id")(prop)


def is_primary_ref(prop: Prop) -> bool:
    return is_ref(prop) and has_sys_tag(prop)


def is_content(prop: Prop) -> bool:
    return has_sys_tag(prop) and fact_eq("content")(prop)


def has_value(prop: Prop) -> bool:
    return not value_eq("")(prop)


def Tag(tag: str) -> Prop:
    return Prop(tag=tag, fact="", value="")


def FactFlag(tag: str, fact: str) -> Prop:
    return Prop(tag=tag, fact=fact, value="")


def FactValue(tag: str, fact: str, value: str) -> Prop:
    return Prop(tag=tag, fact=fact, value=value)


def FactRef(tag: str, ref: str) -> Prop:
    return Prop(tag=tag, fact="id", value=ref)


def Ref(ref: str) -> Prop:
    return FactRef("db", ref)


def Content(value: str) -> Prop:
    return FactValue(tag="db", fact="content", value=value)


@dataclass(frozen=True)
class Item:
    """
    An item is a group of facts at a point in time
    """
    props: frozenset[Prop]

    @property
    def ref(self) -> str:
        for f in filter(is_primary_ref, self.props):
            return f.value
        else:
            raise Exception("No ref")

    @property
    def content(self) -> str:
        return str(next(filter(is_content, self.props), ""))

    def __str__(self) -> str:
        content = self.content
        facts = [str(d) for d in self.facts()]
        if content:
            strs = [content] + list(self.tags()) + facts

        return f"@{self.ref} {' '.join(strs)}"

    def as_dict(self) -> Dict[str, Any]:
        i: Dict[str, Any] = {}
        for tag in self.tags():
            i[tag.lstrip("#")] = {}
        if self.content:
            i["db"] = {"content": self.content}
        for f in self.facts():
            i[f.tag][f.fact] = f.value if has_value(f) else True
        return i

    def tags(self) -> Set[str]:
        return {str(Tag(f.tag)) for f in self.props if not has_sys_tag(f)}

    def facts(self) -> Set[Prop]:
        return {f for f in self.props if is_fact(f) and not has_sys_tag(f)}

    def add_props(self, add: Set[Prop]) -> Item:
        return Item(self.props.union(add))
