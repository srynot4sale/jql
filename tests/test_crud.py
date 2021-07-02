def test_basic_create(db) -> None:
    item = {"db": {"content": "go to supermarket"}, "todo": {"completed": ""}}

    resp = db.query_one("CREATE go to supermarket #todo #todo/completed", item)
    db.query_one(f"@{resp.ref}", item)


def test_multiple_creates(db) -> None:
    item1 = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}
    item2 = {"db": {"content": "groceries"}, "chores": {}}

    resp1 = db.query_one("CREATE do dishes #todo #chores", item1)
    db.query_one(f"@{resp1.ref}", item1)

    resp2 = db.query_one("CREATE groceries #chores", item2)
    db.query_one(f"@{resp1.ref}", item1)
    db.query_one(f"@{resp2.ref}", item2)


def test_basic_create_add_tags(db) -> None:
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

    resp = db.query_one("CREATE do dishes #todo #chores", item)
    db.query_one(f"@{resp.ref}", item)

    item["new"] = {}

    db.query_one(f"@{resp.ref} SET #new", item)
    db.query_one(f"@{resp.ref}", item)

    item["another"] = {}

    db.query_one(f"@{resp.ref} SET #another", item)
    db.query_one(f"@{resp.ref}", item)


def test_basic_create_add_facts(db) -> None:
    item = {"db": {"content": "stuff"}, "chores": {}}

    resp = db.query_one("CREATE stuff #chores", item)
    db.query_one(f"@{resp.ref}", item)

    item["todo"] = {"immediately": ""}

    db.query_one(f"@{resp.ref} SET #todo/immediately", item)
    db.query_one(f"@{resp.ref}", item)

    item["todo"]["nottomorrow"] = ""

    db.query_one(f"@{resp.ref} SET #todo/nottomorrow", item)
    db.query_one(f"@{resp.ref}", item)


def test_basic_tags_normalized(db) -> None:
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

    resp = db.query_one("CREATE do dishes #todo #chores", item)
    db.query_one(f"@{resp.ref}", item)

    item["new"] = {}

    db.query_one(f"@{resp.ref} SET #new", item)
    db.query_one(f"@{resp.ref}", item)

    # Re-adding same tag shouldn't create two
    db.query_one(f"@{resp.ref} SET #new", item)
    db.query_one(f"@{resp.ref}", item)


def test_list(db) -> None:
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}
    resp = db.query_one("CREATE do dishes #todo #chores", item)
    db.query_one(f"@{resp.ref}", item)

    db.query("#chores", [item])
    db.query("#todo", [item])
    db.query("#notrealtag", [])
    db.query("do dishes", [item])
    db.query("dish", [item])
    db.query("dush", [])
    db.query("#todo #chores", [item])
    db.query("#todo #fake", [])

    item2 = {"db": {"content": "stuff"}, "chores": {"late": "yes"}}
    resp = db.query_one("CREATE stuff #chores/late=yes", item2)
    db.query_one(f"@{resp.ref}", item2)

    db.query("#todo", [item])
    db.query("#chores", [item, item2])
    db.query("stuff", [item2])
    db.query("#chores/late", [item2])
    db.query("#chores/late=yes", [item2])
    db.query("#chores/late=no", [])


def test_list_by_content(db) -> None:
    item = {"db": {"content": "do dishes for batman"}, "todo": {}, "chores": {}}
    item2 = {"db": {"content": "tears for bATman"}, "chores": {"late": "yes"}}
    resp = db.query_one("CREATE do dishes for batman #todo #chores", item)
    resp2 = db.query_one("CREATE tears for bATman #chores/late=yes", item2)
    db.query_one(f"@{resp.ref}", item)
    db.query_one(f"@{resp2.ref}", item2)

    db.query("#chores", [item, item2])
    db.query("#todo", [item])
    db.query("do dishes", [item])
    db.query("dish", [item])
    db.query("nopenope", [])
    db.query("for batman", [item, item2])
    db.query("for BATMAN", [item, item2])
    db.query("for", [item, item2])
    db.query("bat", [item, item2])
    db.query("MAN", [item, item2])
