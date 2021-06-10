from __future__ import annotations
import datetime
import structlog
import typing

if typing.TYPE_CHECKING:
    from jql.db import Store

from jql.parser import jql_parser, JqlTransformer
from jql.types import Ref, Prop, Item
import jql.changeset as changeset


log = structlog.get_logger()


class Transaction:
    def __init__(self, store: Store):
        self.timestamp = str(datetime.datetime.now().timestamp())[:-3]
        self._store = store

        self.changeset: typing.List[changeset.Change] = []
        self.closed = False

    def __repr__(self) -> str:
        return f"Transaction({self.query})"

    def commit(self) -> None:
        self._store.apply_changeset(self.changeset)
        self.closed = True

    def is_closed(self) -> bool:
        return self.closed is True

    def create_item(self, props: typing.Set[Prop]) -> Item:
        item = self._store.new_item(props)
        self.changeset.append(changeset.CreateItem(item=item))
        self.commit()
        return item

    def update_item(self, ref: Ref, props: typing.Set[Prop]) -> Item:
        item = self._add_facts(self.get_item(ref), props)
        self.commit()
        return item

    def _add_facts(self, item: Item, facts: typing.Set[Prop]) -> Item:
        for c in self.changeset:
            if isinstance(c, changeset.CreateItem) and c.item == item:
                c.item = c.item.add_facts(facts)
                break
        else:
            for fact in facts:
                self.changeset.append(changeset.AddFact(item=item, new_fact=fact))

        return item.add_facts(facts)

    def get_item(self, ref: Ref) -> Item:
        item = self._store.get_item(ref)
        if not item:
            raise Exception(f'@{id} does not exist')
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

            if not isinstance(values[0], Ref):
                raise Exception(f"Expected an ID first - got {values[0]}")

            return self.update_item(values[0], values[1:])

        if action in ('get', 'history'):
            if not values:
                raise Exception("No data supplied")

            if not isinstance(values[0], Ref):
                raise Exception(f"Expected a ref first - got {values[0]}")

            return self.get_item(values[0])

#        if action == 'list':
#            if not values:
#                raise Exception("No data supplied")

            # Check each data item as a current fact that matches every search term
#            return self.get_items(values)

        raise Exception(f"Unknown query '{query}'")
