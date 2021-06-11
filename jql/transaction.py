from __future__ import annotations
import datetime
import structlog
from typing import List, Set, TYPE_CHECKING

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
        self.closed = False

    def __repr__(self) -> str:
        return f"Transaction({self.query})"

    def commit(self) -> List[Item]:
        resp = self._store.apply_changeset(self.changeset)
        self.closed = True
        return resp

    def is_closed(self) -> bool:
        return self.closed is True

    def create_item(self, props: Set[Prop]) -> Item:
        item = self._store.new_item(props)
        self.changeset.append(changeset.CreateItem(item=item))
        return self.commit()[0]

    def update_item(self, ref: Prop, props: Set[Prop]) -> Item:
        self._add_props(self.get_item(ref), props)
        return self.commit()[0]

    def _add_props(self, item: Item, props: Set[Prop]) -> Item:
        for c in self.changeset:
            if isinstance(c, changeset.CreateItem) and c.item == item:
                c.item = c.item.add_props(props)
                break
        else:
            for prop in props:
                self.changeset.append(changeset.AddFact(item=item, new_fact=prop))

        return item.add_props(props)

    def get_item(self, ref: Prop) -> Item:
        if not is_ref(ref):
            raise Exception("Not a ref")
        item = self._store.get_item(ref)
        if not item:
            raise Exception(f'{ref} does not exist')
        return item

    def q(self, query: str) -> Item:
        if self.is_closed():
            raise Exception("Transaction already completed")

        self.query = query
        tree = jql_parser.parse(query)
        ast = JqlTransformer().transform(tree)
        action = ast.data
        values = ast.children
        log.msg("Query AST", ast=ast)

        if action == 'create':
            if not values:
                raise Exception("No data supplied")

            return self.create_item(values)

        if action == 'set':
            if len(values) < 2:
                raise Exception("No data supplied")

            return self.update_item(values[0], values[1:])

        if action in ('get', 'history'):
            if not values:
                raise Exception("No data supplied")

            return self.get_item(values[0])

#        if action == 'list':
#            if not values:
#                raise Exception("No data supplied")

            # Check each data item as a current fact that matches every search term
#            return self.get_items(values)

        raise Exception(f"Unknown query '{query}'")
