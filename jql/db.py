from abc import ABC, abstractmethod
from hashids import Hashids  # type: ignore
import string
from typing import List, Optional, Iterable, Set
import uuid


from jql.transaction import Transaction
from jql.types import Fact, Item, is_ref, Ref
from jql.changeset import Change


class Store(ABC):
    def __init__(self, salt: str = "") -> None:
        self._salt = salt if salt else str(uuid.uuid4())
        self._hashstore = Hashids(salt=self._salt, alphabet=string.hexdigits[:16], min_length=6)

    def get_item(self, ref: Fact) -> Optional[Item]:
        if not is_ref(ref):
            raise Exception("No ref supplied for get_item")
        return self._get_item(ref)

    def get_items(self, search: Iterable[Fact]) -> List[Item]:
        return self._get_items(search)

    def get_hints(self, search: str = "") -> List[Item]:
        return self._get_tags_as_items(search)

    def apply_changeset(self, changeset: List[Change]) -> List[Item]:
        resp: List[Item] = []
        for change in changeset:
            if change.item:
                if not change.item.ref:
                    raise Exception("No ref to update item")
                resp.append(self._update_item(change.item, change.facts))
            else:
                new_ref = self.next_ref()
                new_item = Item(facts=frozenset(change.facts.union({new_ref})))
                resp.append(self._create_item(new_item))
        return resp

    def new_transaction(self) -> Transaction:
        return Transaction(self)

    def next_ref(self) -> Fact:
        new_ref = self.id_to_ref(self._item_count())
        if self._get_item(new_ref):
            raise Exception(f"{new_ref} item should not already exist")
        return new_ref

    def ref_to_id(self, ref: Fact) -> int:
        return int(self._hashstore.decode(ref.value)[0])

    def id_to_ref(self, i: int) -> Fact:
        return Ref(self._hashstore.encode(i))

    @abstractmethod
    def _get_tags_as_items(self, prefix: str = '') -> List[Item]:
        pass

    @abstractmethod
    def _get_item(self, ref: Fact) -> Optional[Item]:
        pass

    @abstractmethod
    def _get_items(self, search: Iterable[Fact]) -> List[Item]:
        pass

    @abstractmethod
    def _create_item(self, item: Item) -> Item:
        pass

    @abstractmethod
    def _update_item(self, item: Item, new_facts: Set[Fact]) -> Item:
        pass

    @abstractmethod
    def _item_count(self) -> int:
        pass
