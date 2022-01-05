from jql.types import Content, Flag, Value, Item, Ref, Tag, get_content, get_ref, get_tags, get_props, get_flags


def test_content() -> None:
    i = Item({Content("here it is")})
    assert str(get_content(i)) == "here it is"

    j = Item({Content("here is no 2")})
    assert str(get_content(i)) == "here it is"
    assert str(get_content(j)) == "here is no 2"


def test_ref() -> None:
    i = Item({Ref("23"), Content("here it is")})
    assert str(get_ref(i)) == "@23"


def test_tags() -> None:
    i = Item({Ref("23"), Content("here")})
    assert get_tags(i) == set()

    j = Item({Tag("help"), Flag("test", "passed"), Content("here")})
    assert get_tags(j) == set({Tag("help"), Tag("test")})

    k = Item({Tag("help"), Tag("me"), Flag("test", "passed"), Content("here")})
    assert get_tags(k) == set({Tag("help"), Tag("me"), Tag("test")})


def test_props() -> None:
    i = Item({Ref("23"), Flag("test", "passed"), Content("here")})
    assert get_props(i) == set({Flag("test", "passed")})

    j = Item({Tag("help"), Flag("test", "passed"), Content("here")})
    assert get_props(j) == set({Flag("test", "passed")})

    k = Item({Tag("help"), Tag("me"), Flag("test", "passed"), Value("lost", "bet", "twice"), Content("here")})
    assert get_props(k) == set({Flag("test", "passed"), Value("lost", "bet", "twice")})


def test_flags() -> None:
    i = Item({Ref("23"), Flag("test", "passed"), Content("here")})
    assert get_flags(i) == set({Flag("test", "passed")})

    j = Item({Value("help", "me", "now"), Flag("test", "passed"), Content("here")})
    assert get_flags(j) == set({Flag("help", "me"), Flag("test", "passed")})

    k = Item({Value("help", "me", "now"), Flag("test", "passed"), Value("lost", "bet", "twice"), Content("here")})
    assert get_flags(k) == set({Flag("help", "me"), Flag("test", "passed"), Flag("lost", "bet")})


def test_tuples() -> None:
    i = Item({Ref("23"), Flag("test", "passed"), Content("here")})
    assert i.as_tuples() == {("test", "passed", ""), ("db", "content", "here")}

    j = Item({Tag("help"), Flag("test", "passed"), Content("here")})
    assert j.as_tuples() == {("help", "", ""), ("test", "passed", ""), ("db", "content", "here")}

    k = Item({Tag("help"), Tag("me"), Flag("test", "passed"), Value("lost", "bet", "twice"), Content("here")})
    assert k.as_tuples() == {("help", "", ""), ("me", "", ""), ("test", "passed", ""), ("lost", "bet", "twice"), ("db", "content", "here")}

    m = Item({Tag("help"), Tag("me"), Flag("test", "passed"), Value("test", "failed", "twice"), Content("here")})
    assert m.as_tuples() == {("help", "", ""), ("me", "", ""), ("test", "passed", ""), ("test", "failed", "twice"), ("db", "content", "here")}

    # andback
