def test_basic_create(db):
    item = {"db": {"content": "go to supermarket"}, "todo": {"completed": True}}

    resp = db.query("CREATE go to supermarket #todo #todo/completed", item)
    db.query(f"GET @{resp.id}", item)


def test_multiple_creates(db):
    item1 = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}
    item2 = {"db": {"content": "groceries"}, "chores": {}}

    resp1 = db.query("CREATE do dishes #todo #chores", item1)
    db.query(f"GET @{resp1.id}", item1)

    resp2 = db.query("CREATE groceries #chores", item2)
    db.query(f"GET @{resp1.id}", item1)
    db.query(f"GET @{resp2.id}", item2)


def test_basic_create_add_tags(db):
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

    resp = db.query("CREATE do dishes #todo #chores", item)
    db.query(f"GET @{resp.id}", item)

    item["new"] = {}

    db.query(f"SET @{resp.id} #new", item)
    db.query(f"GET @{resp.id}", item)

    item["another"] = {}

    db.query(f"SET @{resp.id} #another", item)
    db.query(f"GET @{resp.id}", item)


def test_basic_create_add_facts(db):
    item = {"db": {"content": "stuff"}, "chores": {}}

    resp = db.query("CREATE stuff #chores", item)
    db.query(f"GET @{resp.id}", item)

    item["todo"] = {"immediately": True}

    db.query(f"SET @{resp.id} #todo/immediately", item)
    db.query(f"GET @{resp.id}", item)

    item["todo"]["nottomorrow"] = True

    db.query(f"SET @{resp.id} #todo/nottomorrow", item)
    db.query(f"GET @{resp.id}", item)


def test_basic_tags_normalized(db):
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

    resp = db.query("CREATE do dishes #todo #chores", item)
    db.query(f"GET @{resp.id}", item)

    item["new"] = {}

    db.query(f"SET @{resp.id} #new", item)
    db.query(f"GET @{resp.id}", item)

    # Re-adding same tag shouldn't create two
    db.query(f"SET @{resp.id} #new", item)
    db.query(f"GET @{resp.id}", item)
