import datetime
import structlog

from jql.parser import jql_parser, JqlTransformer, ItemRef, Tag, Fact, FactValue, Content
import jql.changeset as changeset


log = structlog.get_logger()


class Transaction:
    def __init__(self, user, client, query):
        self.timestamp = str(datetime.datetime.now().timestamp())[:-3]
        self.query = query
        self.user = user
        self.client = client
        self._store = client.store

        self.changeset = []
        self.closed = False
        self.response = self.q(self.query)
        self.commit()

    def __repr__(self):
        return f"Transaction({self.query})"

    def commit(self):
        self._store.apply_changeset(self)
        self.closed = True

    def is_closed(self):
        return self.closed is True

    def create_item(self, content=None):
        item = self._store.new_item(self)
        self.changeset.append(changeset.CreateItem(id=item.id, facts=item.facts))

        if content is not None:        
            item = self.add_facts(item, {content})

        return item

    def add_facts(self, item, facts):
        for c in self.changeset:
            if isinstance(c, changeset.CreateItem) and c.id == item.id:
                c.facts |= set(facts)
                break
        else:
            for fact in facts:
                self.changeset.append(changeset.AddFact(item=item, new_fact=fact))

        return self._store.add_facts(self, item, facts)

    def get_item(self, id):
        id = id.id if isinstance(id, ItemRef) else id
        item = self._store.get_item(self, id)
        if not item:
            raise Exception(f'@{id} does not exist')
        return item

    def get_many(self, terms):
        return self._store.get_items(self, terms)

    def q(self, query):
        if self.is_closed():
            raise Exception("Transaction already completed")

        self.query = query
        tree = jql_parser.parse(query)
        ast = JqlTransformer().transform(tree)
        action = ast.data
        values = ast.children
        log.msg("Query AST", ast=ast)

        if action == 'create':
            if not values:
                raise Exception("No data supplied")

            content = None
            for value in values:
                if isinstance(value, ItemRef):
                    raise Exception("Not accepting ID's in a create")
                if isinstance(value, Content):
                    content = value

            item = self.create_item(content)
            facts = [v for v in values if not isinstance(v, Content)]

            if facts:
                item = self.add_facts(item, facts)

            self.commit()
            return item

        if action == 'set':
            if len(values) < 2:
                raise Exception("No data supplied")

            if not isinstance(values[0], ItemRef):
                raise Exception(f"Expected an ID first - got {values[0]}")

            item = self.get_item(values[0])
            item = self.add_facts(item, values[1:])

            self.commit()
            return item

        if action in ('get', 'history'):
            if not values:
                raise Exception("No data supplied")

            if not isinstance(values[0], ItemRef):
                raise Exception(f"Expected an ID first - got {values[0]}")

            item = self.get_item(values[0])
            # item.print_item(history=(action == 'history'))
            return item

        if action == 'list':
            if not values:
                raise Exception("No data supplied")

            # Check each data item as a current fact that matches every search term
            return self.get_items(values)
