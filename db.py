from dataclasses import dataclass
import datetime


from parser import ItemRef
from user import User
from item import Item


class Store:
    def get_user(self, username):
        return User(username, self)

    def new_transaction(self):
        return DbTransaction(self)

    def new_object(self):
        return Object()

    def get_object(self, id):
        return Object()

    def add_facts(self, obj, facts):
        return Object()


@dataclass()
class Change:
    pass


@dataclass()
class ChangeNewItem(Change):
    id: str
    facts: set


@dataclass()
class ChangeAddFact(Change):
    obj: Item
    fact: any


class DbTransaction:
    def __init__(self, store):
        self.timestamp = str(datetime.datetime.now().timestamp())[:-3]
        self._store = store
        self.changeset = []
        self.closed = False

    def commit(self):
        self._store.apply(self)
        self.closed = True

    def create_item(self, content=None):
        item = self._store.new_item(self)
        self.changeset.append(ChangeNewItem(id=item.id, facts=item.facts))

        if content is not None:        
            item = self.add_facts(item, {content})

        return item

    def add_facts(self, item, facts):
        for c in self.changeset:
            if isinstance(c, ChangeNewItem) and c.id == item.id:
                c.facts |= set(facts)
                break
        else:
            for fact in facts:
                self.changeset.append(ChangeAddFact(item=item, fact=fact))

        return self._store.add_facts(self, item, facts)

    def get_item(self, id):
        id = id.id if isinstance(id, ItemRef) else id
        obj = self._store.get_item(self, id)
        if not obj:
            raise Exception(f'@{id} does not exist')
        return obj

    def add_fact(id, f):
        if f.fact is None:
            self.tx.run(f"MATCH (a:db) WHERE id(a) = $id SET a{f.db_key}", {"id": int(id)})
        elif f.value is None:
            self.tx.run(f"MATCH (a:db) WHERE id(a) = $id SET a{f.db_key} = true", {"id": int(id)})
        else:
            self.tx.run(f"MATCH (a:db) WHERE id(a) = $id SET a{f.db_key} = $val", {"id": int(id), "val": f.value})

        return get_record(id)

        @property
        def db_key(self):
            return f':{self.tag}' if self.fact is None else f'.{self.tag}_{self.fact}'

