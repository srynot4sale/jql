import pytest
import structlog
import typing


from jql.client import Client
from jql.memory import MemoryStore
from jql.types import Item


log = structlog.get_logger()


class dbclass:
    client: Client

    def query(self, query: str, expected: typing.Dict[str, typing.Any]) -> Item:
        log.msg("New transasction", query=query)

        tx = self.client.store.new_transaction()
        response = tx.q(query)

        log.msg("Response", response=response)
        if tx.changeset:
            log.msg("Changeset", changeset=tx.changeset)

        if response.as_dict() != expected:
            log.msg("Response", response=response.as_dict())
            log.msg("Expected", response=expected)

        assert response.as_dict() == expected

        return response


@pytest.fixture
def db() -> typing.Iterator[dbclass]:
    client = Client(store=MemoryStore(), client="pytest:testuser")

    # Delete all in db
    wrapper = dbclass()
    wrapper.client = client
    yield wrapper
