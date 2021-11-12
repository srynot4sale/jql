from abc import ABC, abstractmethod
from hashids import Hashids  # type: ignore
import string
from typing import List, Optional, Iterable, Set
import uuid


from jql.transaction import Transaction
from jql.types import Fact, Item, is_ref, Ref
from jql.changeset import ChangeSet


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
        search_terms = search.lstrip('#').split('/', 1)
        if len(search_terms) > 1:
            return self._get_props_as_items(search_terms[0], search_terms[1])
        else:
            return self._get_tags_as_items(search_terms[0])

    def record_changeset(self, changeset: ChangeSet) -> int:
        return self._record_changeset(changeset)

    def apply_changeset(self, changeset_id: int) -> List[Item]:
        changeset = self._load_changeset(changeset_id)

        resp: List[Item] = []
        for change in changeset.changes:
            # create change
            if change.ref:
                resp.append(self._update_item(change.ref, change.facts))
            elif change.uid:
                new_ref = self._next_ref(change.uid)
                new_item = Item(facts=frozenset(change.facts.union({new_ref})))
                resp.append(self._create_item(new_item))
            else:
                raise Exception("Unexpected Change format")
        return resp

    def new_transaction(self) -> Transaction:
        return Transaction(self)

    def ref_to_id(self, ref: Fact) -> int:
        return int(self._hashstore.decode(ref.value)[0])

    def id_to_ref(self, i: int) -> Fact:
        return Ref(self._hashstore.encode(i))

    @abstractmethod
    def _get_tags_as_items(self, prefix: str = '') -> List[Item]:
        pass

    @abstractmethod
    def _get_props_as_items(self, tag: str, prefix: str = '') -> List[Item]:
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
    def _update_item(self, ref: Fact, new_facts: Set[Fact]) -> Item:
        pass

    @abstractmethod
    def _next_ref(self, uid: str) -> Fact:
        pass

    @abstractmethod
    def _record_changeset(self, changeset: ChangeSet) -> int:
        pass

    @abstractmethod
    def _load_changeset(self, changeset_id: int) -> ChangeSet:
        pass
