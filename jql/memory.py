from jql.db import Store, ChangeNewItem, ChangeAddFact
from jql.item import Item
from jql.parser import Tag


class MemoryStore(Store):
    def __init__(self):
        self._transactions = []
        self._items = {}

    def new_item(self, tx):
        new_id = str(len(self._items.keys()))
        if self._items.get(new_id):
            raise Exception(f"{new_id} item should not already exist")
        return Item(new_id, {Tag("db")})

    def add_facts(self, tx, item, facts):
        return Item(item.id, item.facts.union(set(facts)))

    def get_item(self, tx, id):
        return self._items.get(id, None)

    def apply(self, tx):
        for change in tx.changeset:
            if isinstance(change, ChangeNewItem):
                self._items[change.id] = Item(change.id, change.facts)
            elif isinstance(change, ChangeAddFact):
                self._items[change.item.id] = Item(change.item.id, change.item.facts.union({change.fact}))
