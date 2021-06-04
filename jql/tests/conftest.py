import pytest
import structlog

from jql.memory import MemoryStore


log = structlog.get_logger()


class dbclass:
    def query(self, query, expected):
        log.msg("New transasction", query=query)
        tx = self.client.new_transaction(query)
        log.msg("Response", response=tx.response)
        if tx.changeset:
            log.msg("Changeset", changeset=tx.changeset)

        if tx.response.to_dict() != expected:
            log.msg("Response", response=tx.response.to_dict())
            log.msg("Expected", response=expected)
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
