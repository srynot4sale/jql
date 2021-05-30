import datetime

from parser import jql_parser, JqlTransformer, ItemRef, Tag, Fact, FactValue, Content


class Transaction:
    def __init__(self, user, client, query):
        self.query = query
        self.user = user
        self.client = client
        self.tx = self.client.store.new_transaction()
        self.changeset = {}

        self.response = self.q(self.query)
        self.commit()

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
            TRANSACTIONS[new_id] = self

        return self._id

    @id.setter
    def id(self, v):
        raise Exception("Cannot set transaction id manually")

    def commit(self):
        self.changeset = self.tx.commit()

    def is_closed(self):
        return self.tx.closed is True

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

    def get_many(self, tags=None, facts=None):
        tags = tags or []
        facts = facts or []
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
        tree = jql_parser.parse(query)
        ast = JqlTransformer().transform(tree)
        print(ast)
        action = ast.data
        values = ast.children
        print(values)

        if action == 'create':
            if not values:
                raise Exception("No data supplied")

            content = None
            for value in values:
                if isinstance(value, ItemRef):
                    raise Exception("Not accepting ID's in a create")
                if isinstance(value, Content):
                    content = value

            item = self.tx.create_item(content)
            facts = [v for v in values if not isinstance(v, Content)]

            if facts:
                item = self.tx.add_facts(item, facts)

            self.commit()
            return item

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

            if not isinstance(values[0], ItemRef):
                raise Exception(f"Expected an ID first - got {values[0][0]}")

            item = self.tx.get_item(values[0])
            # item.print_item(history=(action == 'history'))
            return item

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
