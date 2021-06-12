from abc import ABC, abstractmethod
from typing import List, Optional, Iterable


from jql.transaction import Transaction
from jql.types import Tag, Prop, Item, Ref, is_ref
from jql.changeset import Change, CreateItem, AddFact


class Store(ABC):
    def get_item(self, ref: Prop) -> Optional[Item]:
        return self._get_item(ref)

    def new_item(self, props: Iterable[Prop]) -> Item:
        return Item(props=frozenset({Tag("db")})).add_props(props)

    def set_ref(self, item: Item, new_ref: Prop) -> Item:
        if next(filter(is_ref, item.props), False):
            raise Exception("Already has an ID")
        return item.add_props({Ref(new_ref.value)})

    def apply_changeset(self, changeset: List[Change]) -> List[Item]:
        resp: List[Item] = []
        for change in changeset:
            if isinstance(change, CreateItem):
                resp.append(self._create_item(change.item))
            elif isinstance(change, AddFact):
                resp.append(self._update_item(change.item.add_props({change.new_fact})))
        return resp

    def new_transaction(self) -> Transaction:
        return Transaction(self)

    @abstractmethod
    def _get_item(self, ref: Prop) -> Optional[Item]:
        pass

    @abstractmethod
    def _create_item(self, new_item: Item) -> Item:
        pass

    @abstractmethod
    def _update_item(self, updated_item: Item) -> Item:
        pass
