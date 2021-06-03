import pytest

from jql.memory import MemoryStore


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
