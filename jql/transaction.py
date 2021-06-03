import datetime

from jql.parser import jql_parser, JqlTransformer, ItemRef, Tag, Fact, FactValue, Content


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

            if not isinstance(values[0], ItemRef):
                raise Exception(f"Expected an ID first - got {values[0]}")

            item = self.tx.get_item(values[0])
            item = self.tx.add_facts(item, values[1:])

            self.commit()
            return item

        if action in ('get', 'history'):
            if not values:
                raise Exception("No data supplied")

            if not isinstance(values[0], ItemRef):
                raise Exception(f"Expected an ID first - got {values[0]}")

            item = self.tx.get_item(values[0])
            # item.print_item(history=(action == 'history'))
            return item

        if action == 'list':
            if not values:
                raise Exception("No data supplied")

            # Check each data item as a current fact that matches every search term
            return self.tx.get_items(values)
