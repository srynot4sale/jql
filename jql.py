#!./venv/bin/python3

import copy
from dataclasses import dataclass
import datetime

from neo4j import GraphDatabase
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

console = Console()
print = console.print

TRANSACTIONS = {}


class User:
    def __init__(self, name):
        self.name = name
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test"))

    def get_client(self, client, tx="HEAD"):
        session = self.driver.session()
        return Client(self, session=session, name=client, tx=tx)


class Client:
    def __init__(self, user, session, name, tx):
        self.user = user
        self.session = session
        self.name = name
        self.tx = tx

    def new_transaction(self):
        return Transaction(user=self.user, client=self)


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
        result = self.run(f"MATCH (a:{qlabels}) {qwhere} RETURN id(a) AS node_id", {})
        items = []
        for r in result:
            items.append(self.get_item(r['node_id']))
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


class Item:
    def __init__(self, tx, id, exists=False):
        self.tx = tx
        self.id = id
        self._facts = []
        self._current = []
        if exists:
            result = self.tx.get_one(f"MATCH (a:db) WHERE id(a) = $id RETURN a", {"id": int(self.id)})
            if not result:
                raise Exception(f'@{self.id} does not exist')

    def __repr__(self):
        f = ',\n\t\t'.join([str(f) for f in self.get_facts(history=True)])
        return f"Item(\n\tid={self.id},\n\tfacts=[\n\t\t{f}\n\t])"

    def set_content(self, tx, content):
        self.add_fact(tx, 'db', 'content', content)

    def get_facts(self, history=False):
        self._facts = []
        result = self.tx.get_one(f"MATCH (a:db) WHERE id(a) = $id RETURN a", {"id": int(self.id)})
        #print(result)
        #print(result[0].labels)
        for tag in result[0].labels:
            self._facts.append(Fact(id=self.id, tag=tag, fact=None, value=None, tx="", created=""))

        for prop, val in result[0].items():
            tag, fact = prop.split('_', 1)
            self._facts.append(Fact(id=self.id, tag=tag, fact=fact, value=val if val != True else None, tx="", created=""))

        return self._facts#if history else self._current

    def _save_fact(self, f):

        if f.fact is None:
            result = self.tx.run(f"MATCH (a:db) WHERE id(a) = $id SET a{f.db_key}", {"id": int(self.id)})
        elif f.value is None:
            result = self.tx.run(f"MATCH (a:db) WHERE id(a) = $id SET a{f.db_key} = true", {"id": int(self.id)})
        else:
            result = self.tx.run(f"MATCH (a:db) WHERE id(a) = $id SET a{f.db_key} = $val", {"id": int(self.id), "val": f.value})

    def add_tag(self, tx, tag):
        t = Fact(id=self.id, tag=tag, fact=None, value=None, tx=tx.id, created=tx.timestamp)
        self._save_fact(t)

    def add_fact(self, tx, tag, fact=None, value=None, special=False):
        if tag == 'db' and fact == 'id' and not special:
            raise Exception("Cannot change fact #db/id")

        # If we are adding a fact, check if already has the tag set or not
        if fact is not None and tag not in self.get_tags():
            self.add_tag(tx, tag)

        f = Fact(id=self.id, tag=tag, fact=fact, value=value, tx=tx.id, created=tx.timestamp)
        self._save_fact(f)

    def get_tags(self):
        return set(f.tag for f in self.get_facts() if f.is_tag())

    def summary(self, markup=True):
        content = None
        facts = []
        for f in self.get_facts():
            if f.tag == "db" and (f.fact is None or f.fact == "id"):
                continue
            if f.is_content():
                content = f.as_string(markup=markup)
            else:
                facts.append(f.as_string(markup=markup))

        if content is not None:
            facts.insert(0, content)

        if markup:
            return f"[deep_sky_blue1][bold]@[/bold]{self.id}[/deep_sky_blue1] {' '.join(facts)}"
        else:
            return f"@{self.id} {' '.join(facts)}"

    def print_item(self, history=False):
        table = Table(title=self.summary())
        table.add_column("fact")
        table.add_column("value")
        table.add_column("tx")
        table.add_column("created")

        for f in self.get_facts(history=history):
            table.add_row(f.get_key(), "" if f.value is None else str(f.value), f.tx, f.created)

        print()
        print(table)


@dataclass
class F:
    tag: str
    fact: str
    value: str

    def is_tag(self):
        return self.fact is None

    def is_fact(self):
        return not self.is_tag()

    def get_key(self):
        return self.tag if self.fact is None else f'#{self.tag}/{self.fact}'

    def has_value(self):
        return self.value is not None

    @property
    def db_key(self):
        return f':{self.tag}' if self.fact is None else f'.{self.tag}_{self.fact}'

    def is_content(self):
        return self.tag == "db" and self.fact == "content"

    def as_string(self, markup=True):
        if self.is_content():
            return self.value

        output = f'[green][bold]#[/bold]{self.tag}[/green]' if markup else f'#{self.tag}'
        if self.is_fact():
            output += f'/[orange1]{self.fact}[/orange1]' if markup else f'/{self.fact}'
            if self.has_value():
                output += f'=[yellow]{self.value}[/yellow]' if markup else f'={self.value}'

        return output


@dataclass
class Fact(F):
    id: str
    tag: str
    fact: str
    value: str
    tx: str
    created: str


def parse(query):
    tokens = query.split(' ')

    action = tokens[0].lower()

    raw = []
    curr = []
    values = []
    for token in tokens[1:]:
        if token.startswith('#'):
            if curr:
                raw.append(' '.join(curr))
                curr = []
            raw.append(token)
        elif token.startswith('@'):
            if curr:
                raw.append(' '.join(curr))
                curr = []
            raw.append(token)
        else:
            curr.append(token)
    if curr:
        raw.append(' '.join(curr))

    for r in raw:
        if r.startswith('@'):
            values.append(('id', r.lstrip('@')))
            continue

        if not r.startswith('#'):
            values.append(('content', r))
            continue

        if "/" in r:
            tag, fact = r.split("/", 1)
        else:
            tag = r
            fact = None

        tag = tag.lstrip('#')
        if fact is None:
            values.append(('fact', (tag, None, None)))
        else:
            if '=' in fact:
                f, v = fact.split('=', 1)
                values.append(('fact', (tag, f, v)))
            else:
                values.append(('fact', (tag, fact, None)))

    #print(f'action: {action}')
    #print(f'raw: {raw}')
    #print(f'values: {values}')
    return (query, action, raw, values)



examples = [
    "CREATE go to supermarket #todo #todo/completed",
    "CREATE do dishes #todo #chores",
    "CREATE book appointment #todo #todo/remind_at=20210412",
    "SET @40 #todo/completed",
    "SET @41 book appointment at physio",
    "GET @41",
    "HISTORY @41",
    "LIST #todo/completed",
    "LIST do dishes",
]

aaron = User("aaron")
client = aaron.get_client('jql')
for ex in examples:
    print(ex)
    tx = client.new_transaction()
    tx.q(ex)
    tx.commit()

print('Welcome to JQL')
print('q to quit, h for help')

print(f"Logged in as {aaron.name}, with client {client.name} at {client.tx}")

while True:
    try:
        i = Prompt.ask('')
        if i == "q":
            print('Quitting')
            break

        if i == "h":
            print('Examples:')
            for ex in examples:
                print(f"- {ex}")
            print()
            continue

        tx = client.new_transaction()
        tx.q(i)
        tx.commit()
    except BaseException as e:
        print(f"Error occured: [{e.__class__.__name__}] {e}")


import pprint
print('TRANSACTIONS')
pprint.pprint(TRANSACTIONS)

