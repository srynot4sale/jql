from abc import ABC, abstractmethod


from jql.parser import Tag
from jql.item import Item
from jql.user import User
from jql.changeset import CreateItem, AddFact


class Store(ABC):
    def get_user(self, username):
        return User(username, self)

    def get_item(self, tx, id):
        return self._get_item(tx, id)

    def new_item(self, tx):
        return Item(self._new_item_id(), {Tag("db")})

    def add_facts(self, tx, item, facts):
        return Item(item.id, item.facts.union(set(facts)))

    def apply_changeset(self, tx):
        for change in tx.changeset:
            if isinstance(change, CreateItem):
                self._update_item(change.id, Item(change.id, change.facts))
            elif isinstance(change, AddFact):
                self._update_item(change.item.id, Item(change.item.id, change.item.facts.union({change.new_fact})))

    @abstractmethod
    def _new_item_id(self):
        pass

    @abstractmethod
    def _get_item(self, tx, id):
        pass

    @abstractmethod
    def _update_item(self, id, new_item):
        pass
