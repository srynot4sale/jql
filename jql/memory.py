from hashids import Hashids  # type: ignore
import string
import typing
import uuid


from jql.db import Store
from jql.types import Prop, Ref, Item


class MemoryStore(Store):
    def __init__(self, salt: str = "") -> None:
        self._salt = salt if salt else str(uuid.uuid4())
        self._items: typing.Dict[str, Item] = {}

    def _get_item(self, ref: Prop) -> typing.Optional[Item]:
        return self._items.get(ref.value, None)

    def _create_item(self, item: Item) -> Item:
        hashids = Hashids(salt=self._salt, alphabet=string.hexdigits[:16], min_length=6)
        new_ref = Ref(hashids.encode(len(self._items.keys())))
        if self._get_item(new_ref):
            raise Exception(f"{new_ref} item should not already exist")
        self._items[new_ref.value] = new_item = self.set_ref(item, new_ref)
        return new_item

    def _update_item(self, updated_item: Item) -> Item:
        if not updated_item.ref:
            raise Exception("No ref to update item")
        self._items[updated_item.ref] = updated_item
        return updated_item
