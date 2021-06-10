from abc import ABC, abstractmethod
import typing


from jql.transaction import Transaction
from jql.types import Tag, Prop, Ref, Item, FactId
from jql.changeset import Change, CreateItem, AddFact


class Store(ABC):
    def get_item(self, ref: Ref) -> typing.Optional[Item]:
        return self._get_item(ref)

    def new_item(self, props: typing.Set[Prop]) -> Item:
        return Item(facts={Tag("db")}).add_facts(props)

    def set_ref(self, item: Item, new_ref: Ref) -> Item:
        if item.ref:
            raise Exception("Already has an ID")
        return item.add_facts({FactId("db", new_ref.ref)})

    def apply_changeset(self, changeset: typing.List[Change]) -> None:
        for change in changeset:
            if isinstance(change, CreateItem):
                self._create_item(change.item)
            elif isinstance(change, AddFact):
                self._update_item(change.item.ref, change.item.add_facts({change.new_fact}))

    def new_transaction(self) -> Transaction:
        return Transaction(self)

    @abstractmethod
    def _get_item(self, ref: Ref) -> typing.Optional[Item]:
        pass

    @abstractmethod
    def _create_item(self, new_item: Item) -> Item:
        pass

    @abstractmethod
    def _update_item(self, ref: Ref, new_item: Item) -> None:
        pass
