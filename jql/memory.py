from typing import Dict, List, Iterable, Set, Optional


from jql.changeset import ChangeSet
from jql.db import Store
from jql.types import Fact, Item, is_content, is_tag, is_flag, is_ref, has_value, get_tags, get_props, get_flags, Tag, update_item


class MemoryStore(Store):
    def __init__(self, salt: str = "") -> None:
        super().__init__(salt)

        self._changesets: List[ChangeSet] = []
        self._items: Dict[str, Item] = {}
        self._reflist: Dict[str, str] = {}

    def _get_item(self, ref: Fact) -> Optional[Item]:
        return self._items.get(ref.value, None)

    def _get_items(self, search: Iterable[Fact]) -> List[Item]:
        matches = []
        # Loop through every item
        for _, item in self._items.items():
            match = True
            # For each item, loop through each search term
            for fact in search:
                if is_tag(fact) and fact in get_tags(item):
                    continue
                elif is_flag(fact) and fact in get_flags(item):
                    continue
                elif is_content(fact) and str(fact).lower() in str(item.content).lower():
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

    def _create_item(self, item: Item) -> Item:
        self._items[item.ref.value] = item
        return item

    def _update_item(self, ref: Fact, new_facts: Set[Fact]) -> Item:
        item = self._get_item(ref)
        if not item:
            raise Exception("Could not find item being updated")
        updated_item = update_item(item, new_facts)
        self._items[item.ref.value] = updated_item
        return updated_item

    def _get_tags_as_items(self, prefix: str = '') -> List[Item]:
        tags: List[str] = []
        for _, item in self._items.items():
            for t in get_tags(item):
                if t.tag in tags:
                    continue
                if t.tag.startswith(prefix):
                    tags.append(t.tag)

        return [Item(facts={Tag(t)}) for t in tags]

    def _next_ref(self, uid: str) -> Fact:
        new_ref = self.id_to_ref(len(self._items.keys()))
        if self._get_item(new_ref):
            raise Exception(f"{new_ref} item should not already exist")
        self._reflist[uid] = new_ref.value
        return new_ref

    def _record_changeset(self, changeset: ChangeSet) -> int:
        self._changesets.append(changeset)
        return len(self._changesets) - 1

    def _load_changeset(self, changeset_id: int) -> ChangeSet:
        return self._changesets[changeset_id]
