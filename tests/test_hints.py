from jql.types import Flag, Item, Tag, Value


def test_hints(db) -> None:
    tag_db = Item(facts={Tag("db"), Value("db", "count", "2")})
    tag_do = Item(facts={Tag("do"), Value("db", "count", "1")})
    tag_todo = Item(facts={Tag("todo"), Value("db", "count", "2")})
    tag_tomorrow = Item(facts={Tag("tomorrow"), Value("db", "count", "1")})
    flag_todo = Item(facts={Flag("todo", "waiting"), Value("db", "count", "1")})

    with db.tx() as tx:
        tx.q("HINTS #do")
        db.assert_result([])

    with db.tx() as tx:
        tx.q("CREATE do dishes #todo #chores #chores/done #todo/waiting")

    with db.tx() as tx:
        tx.q("CREATE groceries #do #tomorrow #todo")

    with db.tx() as tx:
        tx.q("HINTS #d")
        db.assert_result([tag_db, tag_do])

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

    with db.tx() as tx:
        tx.q("HINTS #todo/")
        db.assert_result([flag_todo])

    with db.tx() as tx:
        tx.q("HINTS #todo/wait")
        db.assert_result([flag_todo])

    with db.tx() as tx:
        tx.q("HINTS #todo/no")
        db.assert_result([])


def test_hints_after_archive(db) -> None:
    tag_todo = Item(facts={Tag("todo"), Value("db", "count", "2")})
    tag_todo_single = Item(facts={Tag("todo"), Value("db", "count", "1")})
    tag_tomorrow = Item(facts={Tag("tomorrow"), Value("db", "count", "1")})
    flag_todo = Item(facts={Flag("todo", "waiting"), Value("db", "count", "1")})

    with db.tx() as tx:
        tx.q("HINTS #do")
        db.assert_result([])

    with db.tx() as tx:
        tx.q("CREATE do dishes #todo #chores #chores/done #todo/waiting")

    ref = db.last_ref

    with db.tx() as tx:
        tx.q("CREATE groceries #do #tomorrow #todo")

    with db.tx() as tx:
        tx.q("HINTS #todo")
        db.assert_result([tag_todo])

    with db.tx() as tx:
        tx.q("HINTS #to")
        db.assert_result([tag_todo, tag_tomorrow])

    with db.tx() as tx:
        tx.q("HINTS #todo/wait")
        db.assert_result([flag_todo])

    with db.tx() as tx:
        tx.q(f"{ref} SET #db/archived")

    with db.tx() as tx:
        tx.q("HINTS #todo")
        db.assert_result([tag_todo_single])

    with db.tx() as tx:
        tx.q("HINTS #to")
        db.assert_result([tag_todo_single, tag_tomorrow])

    with db.tx() as tx:
        tx.q("HINTS #todo/wait")
        db.assert_result([])
