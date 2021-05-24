import pytest

from rich.console import Console

from user import User


console = Console()
print = console.print


class dbclass:
    def query(self, query, expected):
        tx = self.client.new_transaction()
        resp = tx.q(query)
        tx.commit()
        assert resp == expected


@pytest.fixture
def db():
    user = User("testuser", dsn="bolt://db:7687")
    client = user.get_client('testclient')

    # Delete all in db
    wrapper = dbclass()
    wrapper.client = client
    yield wrapper


def test_endtoend(db):
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

    for ex in examples:
        print(ex)
        db.query(ex, "")

