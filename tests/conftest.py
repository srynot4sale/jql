from contextlib import contextmanager
import pytest
import structlog
from typing import Dict, Generator, Iterator, List, Literal, Union


from jql.client import Client
from jql.memory import MemoryStore
from jql.sqlite import SqliteStore
from jql.types import Fact, Item, Ref
from jql.transaction import Transaction


log = structlog.get_logger()

Expected = Dict[str, Dict[str, Union[str, Literal[True]]]]


class dbclass:
    client: Client
    _tx: Transaction
    _last_resp: List[Item]

    @contextmanager
    def tx(self) -> Generator:  # type: ignore
        self._tx = self.client.store.new_transaction()
        yield self._tx
        self._last_resp = self.resp

    @property
    def resp(self) -> List[Item]:
        return self._tx.response

    @property
    def single(self) -> Item:
        assert len(self.resp) == 1
        return self.resp[0]

    @property
    def last_ref(self) -> Fact:
        if hasattr(self, '_tx') and len(self._tx.response):
            res = self._tx.response
        else:
            res = self._last_resp

        assert len(res) == 1

        # Return a copy of the ref
        return Ref(res[0].ref.value)

    def assert_result(self, comparison) -> None:  # type: ignore
        if not isinstance(comparison, List):
            comparison = [comparison]

        assert self.resp == comparison


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
