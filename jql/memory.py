from dataclasses import dataclass
from typing import Dict, List, Iterable, Set, Optional, Tuple


from jql.changeset import ChangeSet
from jql.store import Store
from jql.types import Fact, Flag, Item, is_archived, is_content, is_tag, is_tx, is_flag, is_ref, has_value, get_content, get_ref, get_tags, get_props, get_flags, Tag, revoke_item_facts, update_item, Value


@dataclass
class MemoryRef:
    ref: Fact
    uid: str
    created: str
    archived: bool = False


class MemoryStore(Store):
    def __init__(self, salt: str = "") -> None:
        super().__init__(salt)

        self._changesets: Dict[str, ChangeSet] = {}
        self._items: Dict[str, Item] = {}
        self._reflist: Dict[int, MemoryRef] = {}

    def _get_item(self, ref: Fact) -> Optional[Item]:
        return self._items.get(ref.value, None)

    def _get_items(self, search: Iterable[Fact]) -> List[Item]:
        matches = []
        # Loop through every item
        for _, item in self._items.items():
            if is_tx(item):
                continue
            if is_archived(item):
                continue

            match = True
            # For each item, loop through each search term
            for fact in search:
                if is_tag(fact) and fact in get_tags(item):
                    continue
                elif is_flag(fact) and fact in get_flags(item):
                    continue
                elif is_content(fact) and str(fact).lower() in str(get_content(item)).lower():
                    # Content is a caseless substr match
                    continue
                elif not is_ref(fact) and has_value(fact) and fact in get_props(item):
                    continue
                else:
                    match = False
                    break
            if match:
                matches.append(item)
        return matches

    def _create_item(self, changeset_ref: Fact, item: Item) -> Item:
        self._items[get_ref(item).value] = item
        return item

    def _update_item(self, changeset_ref: Fact, ref: Fact, new_facts: Set[Fact]) -> Item:
        item = self._get_item(ref)
        if not item:
            raise Exception("Could not find item being updated")
        updated_item = update_item(item, new_facts)
        self._items[get_ref(item).value] = updated_item
        if is_archived(updated_item):
            self._reflist[self.ref_to_id(ref)].archived = True
        return updated_item

    def _revoke_item_facts(self, changeset_ref: Fact, ref: Fact, revoke: Set[Fact]) -> Item:
        item = self._get_item(ref)
        if not item:
            raise Exception("Could not find item being updated")
        updated_item = revoke_item_facts(item, revoke)
        self._items[get_ref(item).value] = updated_item
        return updated_item

    def _get_tags_as_items(self, prefix: str = '') -> List[Item]:
        tags: Dict[str, int] = {}
        for _, item in self._items.items():
            if is_tx(item):
                continue
            if is_archived(item):
                continue
            itags = set(get_tags(item))
            itags.add(Tag('db'))
            for t in itags:
                if not t.tag.startswith(prefix):
                    continue
                if t.tag not in tags:
                    tags[t.tag] = 0
                tags[t.tag] += 1

        return [Item(facts={Tag(t), Value('db', 'count', str(tags[t]))}) for t in sorted(tags.keys())]

    def _get_props_as_items(self, tag: str, prefix: str = '') -> List[Item]:
        tags: Dict[str, int] = {}
        for _, item in self._items.items():
            if is_tx(item):
                continue
            if is_archived(item):
                continue
            for f in item.facts:
                if f.tag != tag:
                    continue
                if f.prop == "" and prefix == "":
                    continue
                if not f.prop.startswith(prefix):
                    continue
                if f.prop not in tags:
                    tags[f.prop] = 0
                tags[f.prop] += 1

        return [Item(facts={Flag(tag, t), Value('db', 'count', str(tags[t]))}) for t in sorted(tags.keys())]

    def _next_ref(self, uid: str, created: str, changeset: bool = False) -> Tuple[Fact, int]:
        new_id = len(self._items.keys())
        new_ref = self.id_to_ref(new_id)
        if self._get_item(new_ref):
            raise Exception(f"{new_ref} item should not already exist")
        self._reflist[new_id] = MemoryRef(ref=new_ref, uid=uid, archived=False, created=created)
        return (new_ref, new_id)

    def _record_changeset(self, changeset: ChangeSet) -> str:
        self._changesets[changeset.uuid] = changeset
        return changeset.uuid

    def _load_changeset(self, changeset_uuid: str) -> ChangeSet:
        return self._changesets[changeset_uuid]

    def _get_changesets_as_items(self) -> List[Item]:
        return []

    def _get_history(self, ref: Optional[Fact] = None) -> List[Item]:
        return []
