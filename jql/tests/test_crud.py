def test_basic_create(db):
    item = {"db": {"content": "go to supermarket"}, "todo": {"completed": True}}

    db.query("CREATE go to supermarket #todo #todo/completed", item)
    db.query("GET @0", item)


def test_multiple_creates(db):
    item1 = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}
    item2 = {"db": {"content": "groceries"}, "chores": {}}

    db.query("CREATE do dishes #todo #chores", item1)
    db.query("GET @0", item1)

    db.query("CREATE groceries #chores", item2)
    db.query("GET @0", item1)
    db.query("GET @1", item2)


def test_basic_create_add_tag(db):
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

    db.query("CREATE do dishes #todo #chores", item)
    db.query("GET @0", item)

    item["new"] = {}

    db.query("SET @0 #new", item)
    db.query("GET @0", item)
