from abc import ABC, abstractmethod
from hashids import Hashids  # type: ignore
from huey.contrib.mini import MiniHuey  # type: ignore
import datetime
import json
import string
import os
from typing import List, Optional, Iterable, Set, Tuple
import uuid


from jql.types import Content, Fact, get_content, get_created_time, has_flag, Item, is_ref, Ref, Tag, Value
from jql.changeset import ChangeSet
from jql.tasks import Replicator


class Store(ABC):
    def __init__(self, salt: str = "") -> None:
        self._salt = salt if salt else str(uuid.uuid4())
        self.taskqueue = MiniHuey()
        self.taskqueue.start()
        self.replicator = Replicator(self)

    @property
    def replicate(self) -> bool:
        return os.getenv('REPLICATE', False) is not False

    @property
    def ingest(self) -> bool:
        return os.getenv('INGEST', False) is not False

    @property
    def uuid(self) -> str:
        return self._salt

    def get_last_ingested_changeset(self, dbuuid: str) -> int:
        return self._get_last_ingested_changeset(dbuuid)

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

    def check_changeset_exists(self, changeset_uuid: str) -> bool:
        try:
            self._load_changeset(changeset_uuid)
            return True
        except KeyError:
            return False

    def record_changeset(self, changeset: ChangeSet) -> str:
        if self.check_changeset_exists(changeset.uuid):
            raise Exception(f"Attempt to record a changeset that already exists! {changeset.uuid}, origin: {changeset.origin}")
        return self._record_changeset(changeset)

    def apply_changeset(self, changeset_uuid: str) -> List[Item]:
        changeset = self._load_changeset(changeset_uuid)

        # Make sure we aren't reapplying a changeset
        if changeset.applied:
            raise Exception(f"Attempting to re-apply a changeset! {changeset.uuid}, origin: {changeset.origin}")

        # Commit changeset
        cs_ref, _ = self._next_ref(changeset.uuid, created=str(changeset.created), changeset=True)
        content = json.dumps(changeset.changes_as_dict())

        facts = {
            cs_ref,
            Value('_db', 'created', str(datetime.datetime.now())),
            Tag('_tx'),
            Value('_tx', 'client', changeset.client),
            Value('_tx', 'created', str(changeset.created)),
            Value('_tx', 'uuid', str(changeset.uuid)),
            Value('_tx', 'origin', str(changeset.origin)),
            Content(content),
        }

        if changeset.query:
            facts.add(Value('_tx', 'query', changeset.query))

        cs = Item(facts=facts)
        self._create_item(cs_ref, str(changeset.uuid), cs)

        resp: List[Item] = []
        for change in changeset.changes:
            # if changeset.origin != self.uuid:
            # We'll need to translate refs
            # pass
            if change.revoke:
                resp.append(self._revoke_item_facts(cs_ref, change.uuid, change.facts))
            else:
                # If creating
                if has_flag(iter(change.facts), '_db', 'created'):
                    created = get_created_time(iter(change.facts))

                    new_ref, _ = self._next_ref(change.uuid, created=created.value)
                    new_item = Item(facts=frozenset(change.facts.union({new_ref})))
                    resp.append(self._create_item(cs_ref, change.uuid, new_item))
                # If updating
                else:
                    resp.append(self._update_item(cs_ref, change.uuid, change.facts))

        # Update applied value for changeset
        self._update_changeset(changeset, applied=True)

        # Trigger replication
        self.replicate_changesets()
        return resp

    def replicate_changesets(self) -> None:
        if not self.replicate:
            return

        to_replicate = self._get_unreplicated_changesets()
        if not len(to_replicate):
            return

        for cs in to_replicate:
            result = self.replicator.replicate_changeset(cs)
            if result:
                self._update_changeset(cs, replicated=True)

    def ingest_replication(self) -> None:
        if not self.ingest:
            return

        sources = self.get_items([Tag('_ingest')])
        if not sources:
            return

        for source in sources:
            sourceid = get_content(source).value
            last = self.get_last_ingested_changeset(sourceid)
            for cs in self.replicator.ingest_changesets(sourceid, last):
                # If we have a replication loop, this could be a changeset we've seen before
                if cs.origin == self.uuid:
                    continue
                cid = self.record_changeset(cs)
                self.apply_changeset(cid)

    @classmethod
    def ref_to_id(cls, uuid: str, ref: Fact) -> int:
        return int(Hashids(salt=uuid, alphabet=string.hexdigits[:16], min_length=6).decode(ref.value)[0])

    @classmethod
    def id_to_ref(cls, uuid: str, i: int) -> Fact:
        return Ref(Hashids(salt=uuid, alphabet=string.hexdigits[:16], min_length=6).encode(i))

    def _ref_to_id(self, ref: Fact) -> int:
        return self.ref_to_id(self.uuid, ref)

    def _id_to_ref(self, i: int) -> Fact:
        return self.id_to_ref(self.uuid, i)

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
    def _ref_to_uuid(self, ref: Fact) -> Optional[str]:
        pass

    @abstractmethod
    def _uuid_to_ref(self, uuid: str) -> Optional[Fact]:
        pass

    @abstractmethod
    def _get_item_by_uuid(self, uid: str) -> Optional[Item]:
        pass

    @abstractmethod
    def _get_items(self, search: Iterable[Fact]) -> List[Item]:
        pass

    @abstractmethod
    def _create_item(self, changeset_ref: Fact, uid: str, item: Item) -> Item:
        pass

    @abstractmethod
    def _update_item(self, changeset_ref: Fact, uid: str, new_facts: Set[Fact]) -> Item:
        pass

    @abstractmethod
    def _revoke_item_facts(self, changeset_ref: Fact, uid: str, revoke: Set[Fact]) -> Item:
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
    def _get_unreplicated_changesets(self) -> List[ChangeSet]:
        pass

    @abstractmethod
    def _get_history(self, ref: Optional[Fact] = None) -> List[Item]:
        pass

    @abstractmethod
    def _get_last_ingested_changeset(self, dbuuid: str) -> int:
        pass

    @abstractmethod
    def _update_changeset(self, changeset: ChangeSet, replicated: Optional[bool] = None, applied: Optional[bool] = None) -> None:
        pass
