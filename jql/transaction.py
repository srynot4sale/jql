from __future__ import annotations
import datetime
import structlog
from typing import Iterable, List, TYPE_CHECKING

if TYPE_CHECKING:
    from jql.db import Store

from jql.parser import jql_parser, JqlTransformer
from jql.types import Item, Prop, is_ref
import jql.changeset as changeset


log = structlog.get_logger()


class Transaction:
    def __init__(self, store: Store):
        self.timestamp = str(datetime.datetime.now().timestamp())[:-3]
        self._store = store

        self.changeset: List[changeset.Change] = []
        self.response: List[Item] = []
        self.closed = False

    def __repr__(self) -> str:
        return f"Transaction({self.query})"

    def commit(self) -> None:
        self.response = self._store.apply_changeset(self.changeset)
        self.closed = True

    def is_closed(self) -> bool:
        return self.closed is True

    def create_item(self, props: Iterable[Prop]) -> None:
        item = self._store.new_item(props)
        self.changeset.append(changeset.CreateItem(item=item))
        self.commit()

    def update_item(self, ref: Prop, props: Iterable[Prop]) -> None:
        self._add_props(self._get_item(ref), props)
        self.commit()

    def get_item(self, ref: Prop) -> None:
        self.response.append(self._get_item(ref))

    def _add_props(self, item: Item, props: Iterable[Prop]) -> Item:
        for c in self.changeset:
            if isinstance(c, changeset.CreateItem) and c.item == item:
                c.item = c.item.add_props(props)
                break
        else:
            for prop in props:
                self.changeset.append(changeset.AddFact(item=item, new_fact=prop))

        return item.add_props(props)

    def _get_item(self, ref: Prop) -> Item:
        if not is_ref(ref):
            raise Exception("Not a ref")
        item = self._store.get_item(ref)
        if not item:
            raise Exception(f'{ref} does not exist')
        return item

    def q(self, query: str) -> List[Item]:
        if self.is_closed():
            raise Exception("Transaction already completed")

        self.query = query
        tree = jql_parser.parse(query)
        ast = JqlTransformer().transform(tree)
        action = ast.data
        values: List[Prop] = [c for c in ast.children if isinstance(c, Prop)]
        log.msg("Query AST", ast=ast.children)

        if action == 'create':
            if not values:
                raise Exception("No data supplied")

            self.create_item(values)
            return self.response

        if action == 'set':
            if len(values) < 2:
                raise Exception("No data supplied")

            self.update_item(values[0], values[1:])
            return self.response

        if action in ('get', 'history'):
            if not values:
                raise Exception("No data supplied")

            self.get_item(values[0])
            return self.response

#        if action == 'list':
#            if not values:
#                raise Exception("No data supplied")

            # Check each data item as a current fact that matches every search term
#            return self.get_items(values)

        raise Exception(f"Unknown query '{query}'")
