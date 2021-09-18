import pickle  # noqa
from typing import Dict, List, Iterable, Set, Optional, BinaryIO


from jql.db import Store
from jql.types import Fact, Item, is_content, is_tag, is_flag, is_ref, has_value, get_tags, get_props, get_flags, update_item


class MemoryStore(Store):
    def __init__(self, salt: str = "") -> None:
        super().__init__(salt)

        # self._transactions = set()
        self._items: Dict[str, Item] = {}

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

    def _update_item(self, item: Item, new_facts: Set[Fact]) -> Item:
        updated_item = update_item(item, new_facts)
        self._items[item.ref.value] = updated_item
        return updated_item

    def _item_count(self) -> int:
        return len(self._items.keys())

    def persist_to_disk(self, f: BinaryIO) -> None:
        pickle.dump((self._salt, self._items), f)

    def read_from_disk(self, f: BinaryIO) -> None:
        self._salt, self._items = pickle.load(f)  # noqa
