from hashids import Hashids  # type: ignore
import pickle  # noqa
import string
import typing
import uuid


from jql.db import Store
from jql.types import Prop, Ref, Item, is_tag, is_flag


class MemoryStore(Store):
    def __init__(self, salt: str = "") -> None:
        self._salt = salt if salt else str(uuid.uuid4())
        self._items: typing.Dict[str, Item] = {}

    def _get_item(self, ref: Prop) -> typing.Optional[Item]:
        return self._items.get(ref.value, None)

    def _get_items(self, search: typing.Iterable[Prop]) -> typing.List[Item]:
        matches = []
        for _, item in self._items.items():
            exclude = False
            for prop in search:
                if is_tag(prop):
                    if str(prop) not in item.tags():
                        exclude = True
                elif prop not in item.props:
                    if is_flag(prop) and prop in item.flags():
                        break
                    exclude = True
            if not exclude:
                matches.append(item)
        return matches

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

    def persist_to_disk(self, f: typing.BinaryIO) -> None:
        pickle.dump((self._salt, self._items), f)

    def read_from_disk(self, f: typing.BinaryIO) -> None:
        self._salt, self._items = pickle.load(f)  # noqa
