import pytest
import structlog
from typing import Dict, Iterator, List, Literal, Union


from jql.client import Client
from jql.memory import MemoryStore
from jql.types import Item


log = structlog.get_logger()

Expected = Dict[str, Dict[str, Union[str, Literal[True]]]]


class dbclass:
    client: Client

    def query(self, query: str, expected: List[Expected]) -> List[Item]:
        log.msg("New transasction", query=query)

        tx = self.client.store.new_transaction()
        response = tx.q(query)

        log.msg("Response", response=response)
        if tx.changeset:
            log.msg("Changeset", changeset=tx.changeset)

        dict_response = [r.as_dict() for r in response]
        if dict_response != expected:
            log.msg("Response", response=dict_response)
            log.msg("Expected", response=expected)

        assert dict_response == expected

        return response

    def query_one(self, query: str, expected: Expected) -> Item:
        return self.query(query, [expected])[0]


@pytest.fixture
def db() -> Iterator[dbclass]:
    client = Client(store=MemoryStore(), client="pytest:testuser")

    # Delete all in db
    wrapper = dbclass()
    wrapper.client = client
    yield wrapper
