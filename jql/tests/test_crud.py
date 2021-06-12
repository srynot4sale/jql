def test_basic_create(db) -> None:
    item = {"db": {"content": "go to supermarket"}, "todo": {"completed": True}}

    resp = db.query_one("CREATE go to supermarket #todo #todo/completed", item)
    db.query_one(f"GET @{resp.ref}", item)


def test_multiple_creates(db) -> None:
    item1 = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}
    item2 = {"db": {"content": "groceries"}, "chores": {}}

    resp1 = db.query_one("CREATE do dishes #todo #chores", item1)
    db.query_one(f"GET @{resp1.ref}", item1)

    resp2 = db.query_one("CREATE groceries #chores", item2)
    db.query_one(f"GET @{resp1.ref}", item1)
    db.query_one(f"GET @{resp2.ref}", item2)


def test_basic_create_add_tags(db) -> None:
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

    resp = db.query_one("CREATE do dishes #todo #chores", item)
    db.query_one(f"GET @{resp.ref}", item)

    item["new"] = {}

    db.query_one(f"SET @{resp.ref} #new", item)
    db.query_one(f"GET @{resp.ref}", item)

    item["another"] = {}

    db.query_one(f"SET @{resp.ref} #another", item)
    db.query_one(f"GET @{resp.ref}", item)


def test_basic_create_add_facts(db) -> None:
    item = {"db": {"content": "stuff"}, "chores": {}}

    resp = db.query_one("CREATE stuff #chores", item)
    db.query_one(f"GET @{resp.ref}", item)

    item["todo"] = {"immediately": True}

    db.query_one(f"SET @{resp.ref} #todo/immediately", item)
    db.query_one(f"GET @{resp.ref}", item)

    item["todo"]["nottomorrow"] = True

    db.query_one(f"SET @{resp.ref} #todo/nottomorrow", item)
    db.query_one(f"GET @{resp.ref}", item)


def test_basic_tags_normalized(db) -> None:
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

    resp = db.query_one("CREATE do dishes #todo #chores", item)
    db.query_one(f"GET @{resp.ref}", item)

    item["new"] = {}

    db.query_one(f"SET @{resp.ref} #new", item)
    db.query_one(f"GET @{resp.ref}", item)

    # Re-adding same tag shouldn't create two
    db.query_one(f"SET @{resp.ref} #new", item)
    db.query_one(f"GET @{resp.ref}", item)


def test_list(db) -> None:
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}
    resp = db.query_one("CREATE do dishes #todo #chores", item)
    db.query_one(f"GET @{resp.ref}", item)

    db.query("LIST #chores", [item])
    db.query("LIST #todo", [item])
    db.query("LIST #notrealtag", [])
    db.query("LIST do dishes", [item])
    db.query("LIST dish", [])
    db.query("LIST #todo #chores", [item])
    db.query("LIST #todo #fake", [])

    item2 = {"db": {"content": "stuff"}, "chores": {"late": "yes"}}
    resp = db.query_one("CREATE stuff #chores/late=yes", item2)
    db.query_one(f"GET @{resp.ref}", item2)

    db.query("LIST #todo", [item])
    db.query("LIST #chores", [item, item2])
    db.query("LIST stuff", [item2])
    db.query("LIST #chores/late", [item2])
    db.query("LIST #chores/late=yes", [item2])
    db.query("LIST #chores/late=no", [])
