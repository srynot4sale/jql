from contextlib import contextmanager
import os
import pprint
import pytest
import structlog
from typing import Dict, Generator, Iterator, List, Literal, Union
from unittest import mock

from jql.client import Client
from jql.store import Store
from jql.store.sqlite import SqliteStore
from jql.types import get_ref, Fact, Item, Ref
from jql.transaction import Transaction


# Hack to show more output in assertions
from _pytest.assertion import truncate
truncate.DEFAULT_MAX_LINES = 9999
truncate.DEFAULT_MAX_CHARS = 9999


pytest.register_assert_rewrite("generator")


log = structlog.get_logger()

Expected = Dict[str, Dict[str, Union[str, Literal[True]]]]


class dbclass:
    client: Client
    store: Store
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
        self.compare_results(self.resp, comparison)

    def compare_results(self, res1, res2) -> None:  # type: ignore
        res1 = [r.as_tuples() for r in res1]
        res2 = [r.as_tuples() for r in res2]
        if res1 == res2:
            return
        print('***** res1 != res2 *****')
        print('res1:')
        for r in res1:
            pprint.pprint(r)
        print('res2:')
        for r in res2:
            pprint.pprint(r)

        assert res1 == res2


@pytest.fixture
def db(request) -> Iterator[dbclass]:  # type: ignore
    client = Client(store=request.param(), client="pytest:testuser")

    # Delete all in db
    wrapper = dbclass()
    wrapper.client = client
    wrapper.store = client.store
    yield wrapper


@pytest.fixture
def replication_enabled(request) -> None:  # type: ignore
    with mock.patch.dict(os.environ, {"REPLICATE": "True", "INGEST": "True"}):
        yield


def pytest_generate_tests(metafunc) -> None:  # type: ignore
    if "db" in metafunc.fixturenames:
        metafunc.parametrize("db", [SqliteStore], indirect=True)

    if "interface" in metafunc.fixturenames:
        metafunc.parametrize("interface", ["query", "api"])
