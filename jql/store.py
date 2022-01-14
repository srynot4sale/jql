from abc import ABC, abstractmethod
from hashids import Hashids
import datetime
import json
import string
from typing import List, Optional, Iterable, Set, Tuple
import uuid


from jql.types import Content, Fact, Flag, get_created_time, Item, is_ref, Ref, Tag, Value
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

    def get_changesets(self) -> List[Item]:
        return self._get_changesets_as_items()

    def record_changeset(self, changeset: ChangeSet) -> str:
        return self._record_changeset(changeset)

    def apply_changeset(self, changeset_uuid: str) -> List[Item]:
        changeset = self._load_changeset(changeset_uuid)

        # Commit changeset
        cs_ref, _ = self._next_ref(changeset.uuid, created=str(changeset.created), changeset=True)
        content = json.dumps([c.to_dict() for c in changeset.changes])

        facts = {
            cs_ref,
            Flag('db', 'tx'),
            Value('db', 'created', str(datetime.datetime.now())),
            Tag('tx'),
            Value('tx', 'client', changeset.client),
            Value('tx', 'created', str(changeset.created)),
            Value('tx', 'uuid', str(changeset.uuid)),
            Content(content),
        }

        if changeset.query:
            facts.add(Value('tx', 'query', changeset.query))

        cs = Item(facts=facts)
        self._create_item(cs_ref, cs)

        resp: List[Item] = []
        for change in changeset.changes:
            # create change
            if change.ref:
                if change.revoke:
                    resp.append(self._revoke_item_facts(cs_ref, change.ref, change.facts))
                else:
                    resp.append(self._update_item(cs_ref, change.ref, change.facts))
            elif change.uid:
                created = get_created_time(iter(change.facts))
                new_ref, _ = self._next_ref(change.uid, created=created.value)
                new_item = Item(facts=frozenset(change.facts.union({new_ref})))
                resp.append(self._create_item(cs_ref, new_item))
            else:
                raise Exception("Unexpected Change format")
        return resp

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
    def _create_item(self, changeset_ref: Fact, item: Item) -> Item:
        pass

    @abstractmethod
    def _update_item(self, changeset_ref: Fact, ref: Fact, new_facts: Set[Fact]) -> Item:
        pass

    @abstractmethod
    def _revoke_item_facts(self, changeset_ref: Fact, ref: Fact, revoke: Set[Fact]) -> Item:
        pass

    @abstractmethod
    def _next_ref(self, uid: str, created: str, changeset: bool = False) -> Tuple[Fact, int]:
        pass

    @abstractmethod
    def _record_changeset(self, changeset: ChangeSet) -> str:
        pass

    @abstractmethod
    def _load_changeset(self, changeset_uuid: str) -> ChangeSet:
        pass

    @abstractmethod
    def _get_changesets_as_items(self) -> List[Item]:
        pass

    @abstractmethod
    def _get_history(self, ref: Optional[Fact] = None) -> List[Item]:
        pass
