from contextlib import contextmanager
import pytest
import structlog
from typing import Dict, Generator, Iterator, List, Literal, Union


from jql.client import Client
from jql.memory import MemoryStore
from jql.sqlite import SqliteStore
from jql.types import get_ref, Fact, Item, Ref
from jql.transaction import Transaction


pytest.register_assert_rewrite("generator")


log = structlog.get_logger()

Expected = Dict[str, Dict[str, Union[str, Literal[True]]]]


class dbclass:
    client: Client
    _tx: Transaction
    _last_resp: List[Item]

    @contextmanager
    def tx(self) -> Generator:  # type: ignore
        self._tx = self.client.new_transaction()
        yield self._tx
        self._last_resp = self._tx.response
        del self._tx

    def q(self, query: str) -> List[Item]:
        with self.tx() as tx:
            return tx.q(query)  # type: ignore

    @property
    def resp(self) -> List[Item]:
        if hasattr(self, '_tx') and self._tx.created:
            return self._tx.response
        else:
            return self._last_resp

    @property
    def single(self) -> Item:
        assert len(self.resp) == 1
        return self.resp[0]

    @property
    def last_ref(self) -> Fact:
        # Return a copy of the ref
        return Ref(get_ref(self.single).value)

    def assert_result(self, comparison) -> None:  # type: ignore
        if isinstance(comparison, Item):
            comparison = [comparison]
        assert len(self.resp) == len(comparison)
        assert [r.as_tuples() for r in self.resp] == [r.as_tuples() for r in comparison]


@pytest.fixture
def db(request) -> Iterator[dbclass]:  # type: ignore
    client = Client(store=request.param(), client="pytest:testuser")

    # Delete all in db
    wrapper = dbclass()
    wrapper.client = client
    yield wrapper


def pytest_generate_tests(metafunc) -> None:  # type: ignore
    if "db" in metafunc.fixturenames:
        metafunc.parametrize("db", [MemoryStore, SqliteStore], indirect=True)

    if "interface" in metafunc.fixturenames:
        metafunc.parametrize("interface", ["query", "api"])
