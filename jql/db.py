from abc import ABC, abstractmethod
from typing import List, Optional, Iterable, Set


from jql.transaction import Transaction
from jql.types import Fact, Item, update_item
from jql.changeset import Change


class Store(ABC):
    def get_item(self, ref: Fact) -> Optional[Item]:
        return self._get_item(ref)

    def get_items(self, search: Iterable[Fact]) -> List[Item]:
        return self._get_items(search)

    def apply_changeset(self, changeset: List[Change]) -> List[Item]:
        resp: List[Item] = []
        for change in changeset:
            if change.item:
                resp.append(self._update_item(update_item(change.item, change.facts)))
            else:
                resp.append(self._create_item(change.facts))
        return resp

    def new_transaction(self) -> Transaction:
        return Transaction(self)

    @abstractmethod
    def _get_item(self, ref: Fact) -> Optional[Item]:
        pass

    @abstractmethod
    def _get_items(self, search: Iterable[Fact]) -> List[Item]:
        pass

    @abstractmethod
    def _create_item(self, facts: Set[Fact]) -> Item:
        pass

    @abstractmethod
    def _update_item(self, updated_item: Item) -> Item:
        pass
