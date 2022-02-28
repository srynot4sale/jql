from jql.types import Content, Flag, Tag, Value


def test_func_removing_tag(db) -> None:
    db.q("CREATE do dishes #todo #imported")
    db.q("CREATE groceries not imported #chores")
    db.q("CREATE remove me #chores #imported")

    res1 = db.q("#imported")
    assert len(res1) == 2

    script = """
    tx = Jql.get_tx()
    tx.update_item(ref, {Jql.Tag('imported')}, true)
    return tx
    """

    with db.tx() as tx:
        item = {
            Content(script),
            Tag("_func"),
            Value("_func", "filter", "#imported"),
            Flag("_func", "execute")
        }
        tx.create_item(item)
        tx.commit()

    db.store.run_functions()

    res2 = db.q("#imported")
    assert res2 == []
