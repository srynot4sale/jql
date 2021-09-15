from jql.types import Content, Flag, Tag


def test_basic_create(db, interface) -> None:
    i_dict = {"db": {"content": "go to supermarket"}, "todo": {"completed": ""}}

    with db.tx() as tx:
        if interface == "query":
            tx.q("CREATE go to supermarket #todo #todo/completed")
        else:
            item = {Content("go to supermarket"), Tag("todo"), Flag("todo", "completed")}
            tx.create_item(item)
            tx.commit()

        db.assert_result(i_dict)

    with db.tx() as tx:
        if interface == "query":
            tx.q(str(db.last_ref))
        else:
            tx.get_item(db.last_ref)

        db.assert_result(i_dict)


def test_multiple_creates(db, interface) -> None:
    i1_dict = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}
    i2_dict = {"db": {"content": "groceries"}, "chores": {}}

    with db.tx() as tx:
        if interface == "query":
            tx.q("CREATE do dishes #todo #chores")
        else:
            item = {Content("do dishes"), Tag("todo"), Tag("chores")}
            tx.create_item(item)
            tx.commit()

        db.assert_result(i1_dict)

    with db.tx() as tx:
        if interface == "query":
            tx.q(str(db.last_ref))
        else:
            tx.get_item(db.last_ref)

        db.assert_result(i1_dict)
        ref1 = db.last_ref

    with db.tx() as tx:
        if interface == "query":
            tx.q("CREATE groceries #chores")
        else:
            item = {Content("groceries"), Tag("chores")}
            tx.create_item(item)
            tx.commit()

        db.assert_result(i2_dict)
        ref2 = db.last_ref

    assert ref1 != ref2

    with db.tx() as tx:
        if interface == "query":
            tx.q(str(ref1))
            db.assert_result(i1_dict)
        else:
            tx.get_item(ref1)
            db.assert_result(i1_dict)

    with db.tx() as tx:
        if interface == "query":
            tx.q(str(ref2))
            db.assert_result(i2_dict)
        else:
            tx.get_item(ref2)
            db.assert_result(i2_dict)


def test_basic_create_add_tags(db, interface) -> None:
    i_dict = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

    with db.tx() as tx:
        if interface == "query":
            tx.q("CREATE do dishes #todo #chores")
        else:
            item = {Content("do dishes"), Tag("todo"), Tag("chores")}
            tx.create_item(item)
            tx.commit()

        db.assert_result(i_dict)

    ref = db.last_ref

    i_dict["new"] = {}

    with db.tx() as tx:
        if interface == "query":
            tx.q(f"{str(ref)} SET #new")
        else:
            item = {Tag("new")}
            tx.update_item(ref, item)
            tx.commit()

        db.assert_result(i_dict)
        assert ref == db.last_ref

    i_dict["another"] = {}

    with db.tx() as tx:
        if interface == "query":
            tx.q(f"{str(ref)} SET #another")
        else:
            item = {Tag("another")}
            tx.update_item(ref, item)
            tx.commit()

        db.assert_result(i_dict)
        assert ref == db.last_ref


def test_basic_create_add_facts(db, interface) -> None:
    i_dict = {"db": {"content": "stuff"}, "chores": {}}

    with db.tx() as tx:
        if interface == "query":
            tx.q("CREATE stuff #chores")
        else:
            item = {Content("stuff"), Tag("chores")}
            tx.create_item(item)
            tx.commit()

        db.assert_result(i_dict)

    ref = db.last_ref

    with db.tx() as tx:
        if interface == "query":
            tx.q(str(ref))
        else:
            tx.get_item(ref)

        db.assert_result(i_dict)

    i_dict["todo"] = {"immediately": ""}

    with db.tx() as tx:
        if interface == "query":
            tx.q(f"{str(ref)} SET #todo/immediately")
        else:
            item = {Flag("todo", "immediately")}
            tx.update_item(ref, item)
            tx.commit()

        db.assert_result(i_dict)
        assert ref == db.last_ref

    i_dict["todo"]["nottomorrow"] = ""

    with db.tx() as tx:
        if interface == "query":
            tx.q(f"{str(ref)} SET #todo/nottomorrow")
        else:
            item = {Flag("todo", "nottomorrow")}
            tx.update_item(ref, item)
            tx.commit()

        db.assert_result(i_dict)
        assert ref == db.last_ref

    with db.tx() as tx:
        if interface == "query":
            tx.q(str(ref))
        else:
            tx.get_item(ref)

        db.assert_result(i_dict)


def test_basic_tags_normalized(db, interface) -> None:
    i_dict = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

    with db.tx() as tx:
        if interface == "query":
            tx.q("CREATE do dishes #todo #chores")
        else:
            item = {Content("do dishes"), Tag("todo"), Tag("chores")}
            tx.create_item(item)
            tx.commit()

        db.assert_result(i_dict)

    ref = db.last_ref

    with db.tx() as tx:
        if interface == "query":
            tx.q(str(ref))
        else:
            tx.get_item(ref)

        db.assert_result(i_dict)

    i_dict["new"] = {}

    with db.tx() as tx:
        if interface == "query":
            tx.q(f"{str(ref)} SET #new")
        else:
            item = {Tag("new")}
            tx.update_item(ref, item)
            tx.commit()

        db.assert_result(i_dict)
        assert ref == db.last_ref

    with db.tx() as tx:
        if interface == "query":
            tx.q(str(ref))
        else:
            tx.get_item(ref)

        db.assert_result(i_dict)

    # Re-adding same tag shouldn't create two
    with db.tx() as tx:
        if interface == "query":
            tx.q(f"{str(ref)} SET #new")
        else:
            item = {Tag("new")}
            tx.update_item(ref, item)
            tx.commit()

        db.assert_result(i_dict)
        assert ref == db.last_ref

    with db.tx() as tx:
        if interface == "query":
            tx.q(str(ref))
        else:
            tx.get_item(ref)

        db.assert_result(i_dict)


def test_list(db) -> None:
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

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

    item2 = {"db": {"content": "stuff"}, "chores": {"late": "yes"}}
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
    item = {"db": {"content": "do dishes for batman"}, "todo": {}, "chores": {}}
    item2 = {"db": {"content": "tears for bATman"}, "chores": {"late": "yes"}}

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
