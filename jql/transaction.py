from __future__ import annotations
import datetime
import structlog
from typing import Iterable, List, TYPE_CHECKING

if TYPE_CHECKING:
    from jql.db import Store

from jql.parser import jql_parser, JqlTransformer
from jql.types import Item, Fact, is_ref
from jql.changeset import Change


log = structlog.get_logger()


class Transaction:
    def __init__(self, store: Store):
        self.timestamp = str(datetime.datetime.now().timestamp())[:-3]
        self._store = store

        self.changeset: List[Change] = []
        self.response: List[Item] = []
        self.closed = False

    def __repr__(self) -> str:
        return f"Transaction({self.query})"

    def commit(self) -> None:
        log.msg("tx.commit()")
        self.response = self._store.apply_changeset(self.changeset)
        self.closed = True

    def is_closed(self) -> bool:
        return self.closed is True

    def create_item(self, facts: Iterable[Fact]) -> None:
        if not facts:
            raise Exception("No data supplied")

        log.msg("tx.create_item()", facts=facts)
        change = Change(item=None, facts=set(facts))
        self.changeset.append(change)

    def update_item(self, ref: Fact, facts: Iterable[Fact]) -> None:
        if not facts:
            raise Exception("No data supplied")

        log.msg("tx.update_item()", ref=ref, facts=facts)
        item = self._get_item(ref)
        change = Change(item=item, facts=set(facts))
        self.changeset.append(change)

    def get_item(self, ref: Fact) -> None:
        log.msg("tx.get_item()", ref=ref)
        self.response.append(self._get_item(ref))

    def get_items(self, search: Iterable[Fact]) -> None:
        if not search:
            raise Exception("No search criteria supplied")
        log.msg("tx.get_items()", search=search)
        self.response.extend(self._get_items(search))

    def get_hints(self, search: str = '') -> None:
        # log.msg("tx.get_hints()", search=search)
        self.response.extend(self._store.get_hints(search))

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
        if self.is_closed():
            raise Exception("Transaction already completed")

        self.query = query
        tree = jql_parser.parse(query)
        ast = JqlTransformer().transform(tree)
        action = ast.data
        values: List[Fact] = [c for c in ast.children if isinstance(c, Fact)]
        # log.msg(f"Query '{query}' AST", ast=ast.children)

        if action == 'create':
            self.create_item(values)
            self.commit()
            return self.response

        if action == 'set':
            self.update_item(values[0], values[1:])
            self.commit()
            return self.response

        if action in ('get', 'history'):
            self.get_item(values[0])
            return self.response

        if action == 'list':
            self.get_items(values)
            return self.response

        if action == 'hints':
            self.get_hints(values[0].tag if values else '')
            return self.response

        raise Exception(f"Unknown query '{query}'")
