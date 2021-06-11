def test_basic_create(db) -> None:
    item = {"db": {"content": "go to supermarket"}, "todo": {"completed": True}}

    resp = db.query("CREATE go to supermarket #todo #todo/completed", item)
    db.query(f"GET @{resp.ref}", item)


def test_multiple_creates(db) -> None:
    item1 = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}
    item2 = {"db": {"content": "groceries"}, "chores": {}}

    resp1 = db.query("CREATE do dishes #todo #chores", item1)
    db.query(f"GET @{resp1.ref}", item1)

    resp2 = db.query("CREATE groceries #chores", item2)
    db.query(f"GET @{resp1.ref}", item1)
    db.query(f"GET @{resp2.ref}", item2)


def test_basic_create_add_tags(db) -> None:
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

    resp = db.query("CREATE do dishes #todo #chores", item)
    db.query(f"GET @{resp.ref}", item)

    item["new"] = {}

    db.query(f"SET @{resp.ref} #new", item)
    db.query(f"GET @{resp.ref}", item)

    item["another"] = {}

    db.query(f"SET @{resp.ref} #another", item)
    db.query(f"GET @{resp.ref}", item)


def test_basic_create_add_facts(db) -> None:
    item = {"db": {"content": "stuff"}, "chores": {}}

    resp = db.query("CREATE stuff #chores", item)
    db.query(f"GET @{resp.ref}", item)

    item["todo"] = {"immediately": True}

    db.query(f"SET @{resp.ref} #todo/immediately", item)
    db.query(f"GET @{resp.ref}", item)

    item["todo"]["nottomorrow"] = True

    db.query(f"SET @{resp.ref} #todo/nottomorrow", item)
    db.query(f"GET @{resp.ref}", item)


def test_basic_tags_normalized(db) -> None:
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

    resp = db.query("CREATE do dishes #todo #chores", item)
    db.query(f"GET @{resp.ref}", item)

    item["new"] = {}

    db.query(f"SET @{resp.ref} #new", item)
    db.query(f"GET @{resp.ref}", item)

    # Re-adding same tag shouldn't create two
    db.query(f"SET @{resp.ref} #new", item)
    db.query(f"GET @{resp.ref}", item)
