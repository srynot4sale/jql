import pytest

from memory import MemoryStore
from parser import jql_parser, JqlTransformer


class dbclass:
    def query(self, query, expected):
        tx = self.client.new_transaction(query)
        print(tx.response)
        print(tx.response.to_dict())
        print(tx.tx.changeset)
        assert tx.response.to_dict() == expected


@pytest.fixture
def db():
    store = MemoryStore()
    user = store.get_user("testuser")
    client = user.get_client('testclient')

    # Delete all in db
    wrapper = dbclass()
    wrapper.client = client
    yield wrapper


examples = [
    "CREATE go to supermarket #todo #todo/completed",
    "CREATE do dishes #todo #chores",
    "CREATE book appointment #todo #todo/remind_at=20210412",
    "SET @2 #todo/completed",
    "SET @3 book appointment at physio",
    "GET @3",
    "HISTORY @3",
    "LIST #todo/completed",
    "LIST do dishes",
]


@pytest.mark.parametrize("test_query", examples)
def test_parser(test_query):
    tree = jql_parser.parse(test_query)
    # print(tree)
    JqlTransformer().transform(tree)


def test_basic_create(db):
    item = {"db": {"content": "go to supermarket"}, "todo": {"completed": True}}

    db.query("CREATE go to supermarket #todo #todo/completed", item)
    db.query("GET @0", item)


def test_basic_create_update(db):
    item = {"db": {"content": "do dishes"}, "todo": {}, "chores": {}}

    db.query("CREATE do dishes #todo #chores", item)
    db.query("GET @0", item)
