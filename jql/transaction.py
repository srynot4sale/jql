from __future__ import annotations
import datetime
import structlog
from typing import Iterable, List, Optional, TYPE_CHECKING
import uuid

if TYPE_CHECKING:
    from jql.client import Client
    from jql.db import Store

from jql.parser import jql_parser, JqlTransformer
from jql.types import Item, Fact, is_ref, has_flag, Value
from jql.changeset import Change, ChangeSet


logger = structlog.get_logger()


class Transaction:
    def __init__(self, client: Client, store: Store):
        self.created: Optional[datetime.datetime] = None
        self._store = store
        self._client = client

        self.query: str = ''
        self.changeset: Optional[ChangeSet] = None
        self.response: List[Item] = []
        self.closed = False
        self.log = logger.bind()

    def __repr__(self) -> str:
        return f"Transaction({self.query})"

    def add_response(self, response: List[Item]) -> None:
        self.log.msg(response)
        self.response.extend(response)

    def commit(self) -> None:
        if self.changeset:
            self.log.msg("tx.commit()", changeset=self.changeset)
            cid = self._store.record_changeset(self.changeset)
            self.add_response(self._store.apply_changeset(cid))
            self.closed = True

    def start(self) -> None:
        if self.is_closed():
            raise Exception("Transaction already completed")
        if not self.created:
            self.created = datetime.datetime.now()

    def is_closed(self) -> bool:
        return self.closed is True

    def create_item(self, facts: Iterable[Fact]) -> None:
        if not facts:
            raise Exception("No data supplied")
        facts = set(facts)

        self.start()
        self.log.msg("tx.create_item()", facts=facts)
        if not has_flag(Item(facts=facts), 'db', 'created'):
            facts.add(Value('db', 'created', str(datetime.datetime.now())))

        self._add_change(Change(uid=str(uuid.uuid4()), facts=facts))

    def update_item(self, ref: Fact, facts: Iterable[Fact], revoke: bool = False) -> None:
        if not facts:
            raise Exception("No data supplied")
        self.start()
        self.log.msg("tx.update_item()", ref=ref, facts=facts, revoke=revoke)
        self._add_change(Change(ref=ref, facts=set(facts), revoke=revoke))

    def get_item(self, ref: Fact) -> None:
        self.start()
        self.log.msg("tx.get_item()", ref=ref)
        self.add_response([self._get_item(ref)])

    def get_items(self, search: Iterable[Fact]) -> None:
        if not search:
            raise Exception("No search criteria supplied")
        self.start()
        self.log.msg("tx.get_items()", search=search)
        self.add_response(self._get_items(search))

    def get_hints(self, search: str = '') -> None:
        self.start()
        # self.log.msg("tx.get_hints()", search=search)
        self.add_response(self._store.get_hints(search))

    def get_changesets(self) -> None:
        self.start()
        self.log.msg("tx.get_changesets()")
        self.add_response(self._store.get_changesets())

    def _add_change(self, change: Change) -> None:
        if not self.created:
            raise Exception("Transaction not started")

        if not self.changeset:
            self.changeset = ChangeSet(
                uuid=str(uuid.uuid4()),
                client=self._client.ref,
                created=self.created,
                query=self.query,
                changes=[]
            )

        self.changeset.changes.append(change)

    def _get_item(self, ref: Fact) -> Item:
        if not is_ref(ref):
            raise Exception("Not a ref")
        item = self._store.get_item(ref)
        if not item:
            raise Exception(f'{ref} does not exist')
        return item

    def _get_items(self, search: Iterable[Fact]) -> List[Item]:
        return self._store.get_items(search)

    def q(self, query: str) -> List[Item]:
        self.start()

        self.query = query
        self.log.msg(f"Query '{query}'")
        self.log = self.log.bind(query=query)
        tree = jql_parser.parse(query)
        ast = JqlTransformer().transform(tree)
        action = ast.data
        values: List[Fact] = [c for c in ast.children if isinstance(c, Fact)]  # type: ignore

        if action == 'create':
            self.create_item(values)
            self.commit()
            return self.response

        if action in ('set', 'del'):
            self.update_item(values[0], values[1:], revoke=(action == 'del'))
            self.commit()
            return self.response

        if action in ('get', 'history'):
            self.get_item(values[0])
            return self.response

        if action == 'list':
            self.get_items(values)
            return self.response

        if action == 'hints':
            search = str(values[0]) if values else ''
            if search and self.query.endswith('/'):
                search += '/'
            self.get_hints(search)
            return self.response

        if action == 'changesets':
            self.get_changesets()
            return self.response

        raise Exception(f"Unknown query '{query}'")
