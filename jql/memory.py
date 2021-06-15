from hashids import Hashids  # type: ignore
import pickle  # noqa
import string
from typing import Dict, List, Iterable, Set, Optional, BinaryIO
import uuid


from jql.db import Store
from jql.types import Fact, Ref, Item, is_content, is_tag, is_flag, is_ref, has_value, get_tags, get_props, get_flags


class MemoryStore(Store):
    def __init__(self, salt: str = "") -> None:
        self._salt = salt if salt else str(uuid.uuid4())
        # self._transactions = set()
        self._items: Dict[str, Item] = {}

    def _get_item(self, ref: Fact) -> Optional[Item]:
        if not is_ref(ref):
            raise Exception("No ref supplied for _get_item")
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
                elif is_content(fact) and str(fact).lower() in item.content.lower():
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

    def _create_item(self, facts: Set[Fact]) -> Item:
        hashids = Hashids(salt=self._salt, alphabet=string.hexdigits[:16], min_length=6)
        new_ref = Ref(hashids.encode(len(self._items.keys())))
        if self._get_item(new_ref):
            raise Exception(f"{new_ref} item should not already exist")
        return self._update_item(Item(facts=frozenset(facts.union({new_ref}))))

    def _update_item(self, updated_item: Item) -> Item:
        if not updated_item.ref:
            raise Exception("No ref to update item")
        self._items[updated_item.ref] = updated_item
        return updated_item

    def persist_to_disk(self, f: BinaryIO) -> None:
        pickle.dump((self._salt, self._items), f)

    def read_from_disk(self, f: BinaryIO) -> None:
        self._salt, self._items = pickle.load(f)  # noqa
