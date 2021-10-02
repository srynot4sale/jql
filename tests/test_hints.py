def test_hints(db) -> None:
    tag_do = {"do": {}}
    tag_todo = {"todo": {}}
    tag_tomorrow = {"tomorrow": {}}

    with db.tx() as tx:
        tx.q("HINTS #do")
        db.assert_result([])

    with db.tx() as tx:
        tx.q("CREATE do dishes #todo #chores")

    with db.tx() as tx:
        tx.q("CREATE groceries #do #tomorrow")

    with db.tx() as tx:
        tx.q("HINTS #d")
        db.assert_result([tag_do])

    with db.tx() as tx:
        tx.q("HINTS #do")
        db.assert_result([tag_do])

    with db.tx() as tx:
        tx.q("HINTS #don")
        db.assert_result([])

    with db.tx() as tx:
        tx.q("HINTS #todo")
        db.assert_result([tag_todo])

    with db.tx() as tx:
        tx.q("HINTS #to")
        db.assert_result([tag_todo, tag_tomorrow])
