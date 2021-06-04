from jql.db import Store


class MemoryStore(Store):
    def __init__(self):
        self._transactions = []
        self._items = {}

    def _new_item_id(self):
        new_id = str(len(self._items.keys()))
        if self._items.get(new_id):
            raise Exception(f"{new_id} item should not already exist")
        return new_id

    def _get_item(self, tx, id):
        return self._items.get(id, None)

    def _update_item(self, id, new_item):
        self._items[id] = new_item
