import datetime
from rich.table import Table
from rich.console import Console

from parser import parse
from item import Item
from fact import F


console = Console()
print = console.print

TRANSACTIONS = {}

class Transaction:
    def __init__(self, user, client):
        self._id = None
        self.timestamp = None
        self.query = None
        self.user = user
        self.client = client
        self.transaction = self.client.session.begin_transaction()

    def __repr__(self):
        return self.query

    @property
    def id(self):
        if not self._id:
            if not self.query:
                raise Exception("No query in transaction")

            new_id = str(len(TRANSACTIONS.keys()))

            if TRANSACTIONS.get(new_id):
                raise Exception(f"{new_id} transaction should not already exist")

            self._id = new_id
            self.timestamp = str(datetime.datetime.now().timestamp())[:-3]
            TRANSACTIONS[new_id] = self

        return self._id

    @id.setter
    def id(self, v):
        raise Exception("Cannot set transaction id manually")

    def commit(self):
        self.transaction.commit()

    def is_closed(self):
        return self.transaction.closed()

    def run(self, query, data):
        return self.transaction.run(query, data)

    def get_one(self, query, data):
        q = self.run(query, data)
        return q.single()

    def get_item(self, id):
        item = Item(self, id, exists=True)
        return item

    def _produce_item(self, id, facts):
        item = Item(self, id)
        item._set_facts(facts)
        return item

    def create_item(self, content=None):
        q = self.get_one("CREATE (a:db) RETURN id(a) AS node_id", {})
        new_id = str(q['node_id'])

        i = Item(self, new_id)
        i.add_fact(self, 'db', 'id', new_id, special=True)
        if content is not None:
            i.set_content(self, content)
        return i

    def get_many(self, tags=[], facts=[]):
        tags.append("db")
        qlabels = ":".join(tags)
        where = []
        for f in facts:
            if f.has_value():
                where.append(f"a{f.db_key} = \"{f.value}\"")
            else:
                where.append(f"a{f.db_key} <> \"\"")
        qwhere = f"WHERE {' AND '.join(where)}" if len(where) else ""
        result = self.run(f"MATCH (a:{qlabels}) {qwhere} RETURN a", {})
        items = []
        for r, in result:
            items.append(self._produce_item(r.id, r))
        return items

    def q(self, query):
        if self.is_closed():
            raise Exception("Transaction already completed")

        self.query = query
        _, action, raw, values = parse(query)

        if action == 'create':
            if not values:
                raise Exception("No data supplied")

            content = None
            for t, value in values:
                if t == "id":
                    raise Exception("Not accepting ID's in a create")
                if t == "content":
                    content = value

            item = self.create_item(content)
            for t, v in values:
                if t == "content":
                    continue
                tag, fact, value = v
                item.add_fact(self, tag, fact, value)

            item.print_item()
            print(f"Created @{item.id}")
            print()

        if action == 'set':
            if len(values) < 2:
                raise Exception("No data supplied")

            if values[0][0] != 'id':
                raise Exception(f"Expected an ID first - got {values[0][0]}")

            _, id = values[0]
            item = self.get_item(id)
            for t, val in values[1:]:
                if t == "content":
                    tag, fact, value = "db", "content", val
                else:
                    tag, fact, value = val
                item.add_fact(self, tag, fact, value)

            item.print_item()
            print(f"Updated @{id}")
            print()

        if action in ('get', 'history'):
            if not values:
                raise Exception("No data supplied")

            if values[0][0] != 'id':
                raise Exception(f"Expected an ID first - got {values[0][0]}")

            _, id = values[0]
            item = self.get_item(id)
            item.print_item(history=(action == 'history'))
            print()

        if action == 'list':
            if not values:
                raise Exception("No data supplied")

            display = []
            tags = []
            facts = []
            for t, val in values:
                if t == "content":
                    display.append(val)
                elif t == "id":
                    raise Exception("Can't list an ID")
                else:
                    tag, f, v = val
                    if f is None:
                        display.append(f'#{tag}')
                        tags.append(tag)
                    elif v is None:
                        display.append(f'#{tag}/{f}')
                        facts.append(F(tag=tag, fact=f, value=None))
                    else:
                        display.append(f'#{tag}/{f}={v}')
                        facts.append(F(tag=tag, fact=f, value=v))

            table = Table(title=f"List all items matching the search terms: {' '.join(display)}")
            table.add_column("item")

            # Check each data item as a current fact that matches every search term
            for item in self.get_many(tags=tags, facts=facts):
                table.add_row(item.summary())

            print()
            print(table)
            print()

