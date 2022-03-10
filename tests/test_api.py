import pytest

from jql.types import Content, Flag, Ref, Tag


def test_basic_create(db) -> None:
    res = db.q("CREATE go to supermarket #todo #todo/completed")

    with db.tx() as tx:
        item = {Content("go to supermarket"), Tag("todo"), Flag("todo", "completed")}
        tx.create_item(item)
        tx.commit()

        db.assert_result(res)


def test_multiple_creates(db) -> None:
    res1 = db.q("CREATE do dishes #todo #chores")

    with db.tx() as tx:
        item = {Content("do dishes"), Tag("todo"), Tag("chores")}
        tx.create_item(item)
        tx.commit()

        db.assert_result(res1)

    res2 = db.q(str(db.last_ref))

    with db.tx() as tx:
        tx.get_item(db.last_ref)

        db.assert_result(res2)
        ref1 = db.last_ref

    res3 = db.q("CREATE groceries #chores")
    with db.tx() as tx:
        item = {Content("groceries"), Tag("chores")}
        tx.create_item(item)
        tx.commit()

        db.assert_result(res3)
        ref2 = db.last_ref

    assert ref1 != ref2

    res4 = db.q(str(ref1))
    with db.tx() as tx:
        tx.get_item(ref1)

        db.assert_result(res4)

    res5 = db.q(str(ref2))
    print(res5)
    with db.tx() as tx:
        tx.get_item(ref2)
        print(tx.response)

        db.assert_result(res5)


def test_basic_create_add_tags(db) -> None:
    res1 = db.q("CREATE do dishes #todo #chores")

    with db.tx() as tx:
        item = {Content("do dishes"), Tag("todo"), Tag("chores")}
        tx.create_item(item)
        tx.commit()

        db.assert_result(res1)

    ref = db.last_ref

    res2 = db.q(f"{str(ref)} SET #new")

    with db.tx() as tx:
        item = {Tag("new")}
        tx.set_facts(ref, item)
        tx.commit()

        db.assert_result(res2)
        assert ref == db.last_ref

    res3 = db.q(f"{str(ref)} SET #another")

    with db.tx() as tx:
        item = {Tag("another")}
        tx.set_facts(ref, item)
        tx.commit()

        db.assert_result(res3)
        assert ref == db.last_ref


def test_basic_create_add_facts(db) -> None:
    res1 = db.q("CREATE stuff #chores")

    with db.tx() as tx:
        item = {Content("stuff"), Tag("chores")}
        tx.create_item(item)
        tx.commit()

        db.assert_result(res1)

    ref = db.last_ref

    res2 = db.q(str(ref))

    with db.tx() as tx:
        tx.get_item(ref)
        db.assert_result(res2)

    res3 = db.q(f"{str(ref)} SET #todo/immediately")

    with db.tx() as tx:
        item = {Flag("todo", "immediately")}
        tx.set_facts(ref, item)
        tx.commit()

        db.assert_result(res3)
        assert ref == db.last_ref

    res4 = db.q(f"{str(ref)} SET #todo/nottomorrow")

    with db.tx() as tx:
        item = {Flag("todo", "nottomorrow")}
        tx.set_facts(ref, item)
        tx.commit()

        db.assert_result(res4)
        assert ref == db.last_ref

    res5 = db.q(str(ref))

    with db.tx() as tx:
        tx.get_item(ref)

        db.assert_result(res5)


def test_basic_tags_normalized(db) -> None:
    res1 = db.q("CREATE do dishes #todo #chores")

    with db.tx() as tx:
        item = {Content("do dishes"), Tag("todo"), Tag("chores")}
        tx.create_item(item)
        tx.commit()

        db.assert_result(res1)

    ref = db.last_ref

    res2 = db.q(str(ref))

    with db.tx() as tx:
        tx.get_item(ref)

        db.assert_result(res2)

    res3 = db.q(f"{str(ref)} SET #new")

    with db.tx() as tx:
        item = {Tag("new")}
        tx.set_facts(ref, item)
        tx.commit()

        db.assert_result(res3)
        assert ref == db.last_ref

    res4 = db.q(str(ref))

    with db.tx() as tx:
        tx.get_item(ref)

        db.assert_result(res4)

    # Re-adding same tag shouldn't create two
    res5 = db.q(f"{str(ref)} SET #new")

    with db.tx() as tx:
        item = {Tag("new")}
        tx.set_facts(ref, item)
        tx.commit()

        db.assert_result(res5)
        assert ref == db.last_ref

    res6 = db.q(str(ref))

    with db.tx() as tx:
        tx.get_item(ref)

        db.assert_result(res6)


def test_add_to_nonexistant_item(db) -> None:
    with db.tx() as tx:
        item = {Content("do dishes"), Tag("todo"), Tag("chores")}
        tx.create_item(item)
        tx.commit()

    ref = db.last_ref

    with db.tx() as tx:
        item = {Tag("new")}
        tx.set_facts(ref, item)
        tx.commit()

        assert ref == db.last_ref

    with db.tx() as tx:
        item = {Tag("another")}

        with pytest.raises(Exception):
            tx.set_facts(Ref('343434'), item)
