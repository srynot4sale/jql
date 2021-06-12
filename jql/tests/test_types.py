from jql.types import Content, FactFlag, FactValue, Item, Ref, Tag


def test_content() -> None:
    i = Item(props=frozenset({Content("here it is")}))
    assert i.content == "here it is"

    j = Item(props=frozenset({Content("here is no 2")}))
    assert i.content == "here it is"
    assert j.content == "here is no 2"


def test_ref() -> None:
    i = Item(props=frozenset({Ref("23"), Content("here it is")}))
    assert i.ref == "23"


def test_tags() -> None:
    i = Item(props=frozenset({Ref("23"), Content("here")}))
    assert i.tags() == set()

    j = Item(props=frozenset({Tag("help"), FactFlag("test", "passed"), Content("here")}))
    assert j.tags() == set({"#help", "#test"})

    k = Item(props=frozenset({Tag("help"), Tag("me"), FactFlag("test", "passed"), Content("here")}))
    assert k.tags() == set({"#help", "#me", "#test"})


def test_facts() -> None:
    i = Item(props=frozenset({Ref("23"), FactFlag("test", "passed"), Content("here")}))
    assert i.facts() == set({FactFlag("test", "passed")})

    j = Item(props=frozenset({Tag("help"), FactFlag("test", "passed"), Content("here")}))
    assert j.facts() == set({FactFlag("test", "passed")})

    k = Item(props=frozenset({Tag("help"), Tag("me"), FactFlag("test", "passed"), FactValue("lost", "bet", "twice"), Content("here")}))
    assert k.facts() == set({FactFlag("test", "passed"), FactValue("lost", "bet", "twice")})


def test_flags() -> None:
    i = Item(props=frozenset({Ref("23"), FactFlag("test", "passed"), Content("here")}))
    assert i.flags() == set({FactFlag("test", "passed")})

    j = Item(props=frozenset({FactValue("help", "me", "now"), FactFlag("test", "passed"), Content("here")}))
    assert j.flags() == set({FactFlag("help", "me"), FactFlag("test", "passed")})

    k = Item(props=frozenset({FactValue("help", "me", "now"), FactFlag("test", "passed"), FactValue("lost", "bet", "twice"), Content("here")}))
    assert k.flags() == set({FactFlag("help", "me"), FactFlag("test", "passed"), FactFlag("lost", "bet")})


def test_dicts() -> None:
    i = Item(props=frozenset({Ref("23"), FactFlag("test", "passed"), Content("here")}))
    assert i.as_dict() == {"test": {"passed": True}, "db": {"content": "here"}}

    j = Item(props=frozenset({Tag("help"), FactFlag("test", "passed"), Content("here")}))
    assert j.as_dict() == {"help": {}, "test": {"passed": True}, "db": {"content": "here"}}

    k = Item(props=frozenset({Tag("help"), Tag("me"), FactFlag("test", "passed"), FactValue("lost", "bet", "twice"), Content("here")}))
    assert k.as_dict() == {"help": {}, "me": {}, "test": {"passed": True}, "lost": {"bet": "twice"}, "db": {"content": "here"}}

    m = Item(props=frozenset({Tag("help"), Tag("me"), FactFlag("test", "passed"), FactValue("test", "failed", "twice"), Content("here")}))
    assert m.as_dict() == {"help": {}, "me": {}, "test": {"passed": True, "failed": "twice"}, "db": {"content": "here"}}
