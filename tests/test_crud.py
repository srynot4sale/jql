from jql.types import Content, Item, Flag, Tag, update_item, Value

interface = "query"


def test_basic_create(db) -> None:
    item = Item(facts={Content("go to supermarket"), Tag("todo"), Flag("todo", "completed")})

    with db.tx() as tx:
        tx.q("CREATE go to supermarket #todo #todo/completed")
        db.assert_result(item)

    with db.tx() as tx:
        tx.q(str(db.last_ref))
        db.assert_result(item)


def test_multiple_creates(db) -> None:
    item1 = Item(facts={Content("do dishes"), Tag("todo"), Tag("chores")})
    item2 = Item(facts={Content("groceries"), Tag("chores")})

    with db.tx() as tx:
        tx.q("CREATE do dishes #todo #chores")
        db.assert_result(item1)

    with db.tx() as tx:
        tx.q(str(db.last_ref))
        db.assert_result(item1)
        ref1 = db.last_ref

    with db.tx() as tx:
        tx.q("CREATE groceries #chores")
        db.assert_result(item2)
        ref2 = db.last_ref

    assert ref1 != ref2

    with db.tx() as tx:
        tx.q(str(ref1))
        db.assert_result(item1)

    with db.tx() as tx:
        tx.q(str(ref2))
        db.assert_result(item2)


def test_basic_create_add_tags(db) -> None:
    item1 = Item(facts={Content("do dishes"), Tag("todo"), Tag("chores")})

    with db.tx() as tx:
        tx.q("CREATE do dishes #todo #chores")
        db.assert_result(item1)

    ref = db.last_ref

    item2 = update_item(item1, {Tag("new")})

    with db.tx() as tx:
        tx.q(f"{str(ref)} SET #new")
        db.assert_result(item2)
        assert ref == db.last_ref

    item3 = update_item(item2, {Tag("another")})

    with db.tx() as tx:
        tx.q(f"{str(ref)} SET #another")
        db.assert_result(item3)
        assert ref == db.last_ref


def test_basic_create_add_facts(db) -> None:
    item1 = Item(facts={Content("stuff"), Tag("chores")})

    with db.tx() as tx:
        tx.q("CREATE stuff #chores")
        db.assert_result(item1)

    ref = db.last_ref

    with db.tx() as tx:
        tx.q(str(ref))
        db.assert_result(item1)

    item2 = update_item(item1, {Flag("todo", "immediately")})

    with db.tx() as tx:
        tx.q(f"{str(ref)} SET #todo/immediately")
        db.assert_result(item2)
        assert ref == db.last_ref

    item3 = update_item(item2, {Flag("todo", "nottomorrow")})

    with db.tx() as tx:
        tx.q(f"{str(ref)} SET #todo/nottomorrow")
        db.assert_result(item3)
        assert ref == db.last_ref

    with db.tx() as tx:
        tx.q(str(ref))
        db.assert_result(item3)


def test_basic_tags_normalized(db) -> None:
    item1 = Item(facts={Content("do dishes"), Tag("todo"), Tag("chores")})

    with db.tx() as tx:
        tx.q("CREATE do dishes #todo #chores")
        db.assert_result(item1)

    ref = db.last_ref

    with db.tx() as tx:
        tx.q(str(ref))
        db.assert_result(item1)

    item2 = update_item(item1, {Tag("new")})

    with db.tx() as tx:
        tx.q(f"{str(ref)} SET #new")
        db.assert_result(item2)
        assert ref == db.last_ref

    with db.tx() as tx:
        tx.q(str(ref))
        db.assert_result(item2)

    # Re-adding same tag shouldn't create two
    with db.tx() as tx:
        tx.q(f"{str(ref)} SET #new")
        db.assert_result(item2)
        assert ref == db.last_ref

    with db.tx() as tx:
        tx.q(str(ref))
        db.assert_result(item2)


def test_list(db) -> None:
    item = Item(facts={Content("do dishes"), Tag("todo"), Tag("chores")})

    with db.tx() as tx:
        tx.q("CREATE do dishes #todo #chores")
        db.assert_result(item)

    with db.tx() as tx:
        tx.q(str(db.last_ref))
        db.assert_result(item)

    def query(q, response):
        with db.tx() as tx:
            tx.q(q)
            db.assert_result(response)

    query("#chores", [item])
    query("#todo", [item])
    query("#notrealtag", [])
    query("do dishes", [item])
    query("dish", [item])
    query("dush", [])
    query("#todo #chores", [item])
    query("#todo #fake", [])

    item2 = Item(facts={Content("stuff"), Value("chores", "late", "yes")})
    with db.tx() as tx:
        tx.q("CREATE stuff #chores/late=yes")
        db.assert_result(item2)

    with db.tx() as tx:
        tx.q(str(db.last_ref))
        db.assert_result(item2)

    query("#todo", [item])
    query("#chores", [item, item2])
    query("stuff", [item2])
    query("#chores/late", [item2])
    query("#chores/late=yes", [item2])
    query("#chores/late=no", [])


def test_list_by_content(db) -> None:
    item = Item(facts={Content("do dishes for batman"), Tag("todo"), Tag("chores")})
    item2 = Item(facts={Content("tears for bATman"), Value("chores", "late", "yes")})

    with db.tx() as tx:
        tx.q("CREATE do dishes for batman #todo #chores")
        db.assert_result(item)
        ref = db.last_ref

    with db.tx() as tx:
        tx.q("CREATE tears for bATman #chores/late=yes")
        db.assert_result(item2)
        ref2 = db.last_ref

    with db.tx() as tx:
        tx.q(str(ref))
        db.assert_result(item)

    with db.tx() as tx:
        tx.q(str(ref2))
        db.assert_result(item2)

    def query(q, response):
        with db.tx() as tx:
            tx.q(q)
            db.assert_result(response)

    query("#chores", [item, item2])
    query("#todo", [item])
    query("do dishes", [item])
    query("dish", [item])
    query("nopenope", [])
    query("for batman", [item, item2])
    query("for BATMAN", [item, item2])
    query("for", [item, item2])
    query("bat", [item, item2])
    query("MAN", [item, item2])
