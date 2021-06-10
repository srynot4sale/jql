from hashids import Hashids
import string
import typing
import uuid


from jql.db import Store
from jql.types import Ref, Item


class MemoryStore(Store):
    def __init__(self, salt: str = "") -> None:
        self._salt = salt if salt else str(uuid.uuid4())
        self._items: typing.Dict[Ref, Item] = {}

    def _get_item(self, ref: Ref) -> typing.Optional[Item]:
        return self._items.get(ref, None)

    def _create_item(self, new_item: Item) -> Item:
        hashids = Hashids(salt=self._salt, alphabet=string.hexdigits[:16], min_length=6)
        new_ref = Ref(hashids.encode(len(self._items.keys())))
        if self._items.get(new_ref):
            raise Exception(f"{new_ref} item should not already exist")
        self._items[new_ref] = self.set_ref(new_item, new_ref)
        return self._items[new_ref]

    def _update_item(self, ref: Ref, new_item: Item) -> None:
        self._items[ref] = new_item
