#!./venv/bin/python3

import copy
from dataclasses import dataclass
import datetime


from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

console = Console()
print = console.print

DATA = {}
TRANSACTIONS = {}

class User:
    def __init__(self, name):
        self.name = name

    def get_client(self, client, tx="HEAD"):
        return Client(self, name=client, tx=tx)


class Client:
    def __init__(self, user, name, tx):
        self.user = user
        self.name = name
        self.tx = tx

    def new_transaction(self):
        tx = Transaction(user=self.user, client=self)
        return tx


class Transaction:
    def __init__(self, user, client):
        self._id = None
        self.timestamp = None
        self.query = None
        self.user = user
        self.client = client
        self.closed = False

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
        self.closed = True

    def is_closed(self):
        return self.closed == True

    def get_item(self, id):
        if id not in DATA.keys():
            raise Exception(f"Cannot find @{id}")

        item = DATA[id]
        return item

    def create_item(self, content=None):
        new_id = str(len(DATA.keys()))
        if DATA.get(new_id):
            raise Exception(f"{new_id} should not already exist")

        i = Item(new_id)
        if content is not None:
            i.set_content(self, content)
        else:
            i.create_empty(self)
        DATA[new_id] = i
        return i

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
            for t, val in values:
                if t == "content":
                    display.append(val)
                elif t == "id":
                    raise Exception("Can't list an ID")
                else:
                    tag, f, v = val
                    if f is None:
                        display.append(f'#{tag}')
                    elif v is None:
                        display.append(f'#{tag}/{f}')
                    else:
                        display.append(f'#{tag}/{f}={v}')

            table = Table(title=f"List all items matching the search terms: {' '.join(display)}")
            table.add_column("item")

            # Check each data item as a current fact that matches every search term
            for id in DATA.keys():
                notfound = False
                item = self.get_item(id).without_history()
                for t, val in values:
                    if notfound:
                        break
                    match = False
                    for fact in item.facts:
                        if t == "content":
                            tag, f, v = "db", "content", val
                        else:
                            tag, f, v = val
                        if tag == fact.tag and f == fact.fact:
                            if t == "content":
                                match = val in fact.value
                            else:
                                match = v == fact.value
                            #if match:
                            #    print(f"Match {val=} == {fact=}")
                            break
                        else:
                            #print(f"No match {val=} != {fact=}")
                            pass
                    if not match:
                        notfound = True

                if not notfound:
                    table.add_row(item.summary())

            print()
            print(table)
            print()

        self.commit()


class Item:
    def __init__(self, id):
        self.id = id
        self.facts = []

    def __repr__(self):
        f = ',\n\t\t'.join([str(f) for f in self.facts])
        return f"Item(\n\tid={self.id},\n\tfacts=[\n\t\t{f}\n\t])"

    def set_content(self, tx, content):
        self.add_fact(tx, 'db', 'content', content)

    def create_empty(self, tx):
        if len(self.facts) != 0:
            raise Exception("Item not empty")
        self.add_tag(tx, 'db')

    def _save_fact(self, f):
        self.facts.append(f)

    def add_tag(self, tx, tag):
        t = Fact(id=self.id, tag=tag, fact=None, value=None, tx=tx.id, created=tx.timestamp)
        self._save_fact(t)

    def add_fact(self, tx, tag, fact=None, value=None):
        # Bad fact
        if 'tag' == 'db' and 'fact' == 'id':
            raise Exception("Cannot change fact #db/id")

        # If we are adding a fact, check if already has the tag set or not
        if fact is not None and tag not in self.get_tags():
            self.add_tag(tx, tag)

        f = Fact(id=self.id, tag=tag, fact=fact, value=value, tx=tx.id, created=tx.timestamp)
        self._save_fact(f)

    def get_tags(self):
        return set(f.tag for f in self.facts if f.is_tag())

    def without_history(self):
        sorted_facts = {}
        for f in self.facts:
            if f.get_key() in sorted_facts.keys():
                # If existing tx is newer, don't update
                if sorted_facts[f.get_key()].tx > f.tx:
                    continue
            sorted_facts[f.get_key()] = f

        item = copy.deepcopy(self)
        item.facts = list(sorted_facts.values())
        return item

    def summary(self, markup=True):
        content = None
        facts = []
        for f in self.facts:
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
        if not history:
            item = self.without_history()
        else:
            item = self

        table = Table(title=item.summary())
        table.add_column("id")
        table.add_column("tx")
        table.add_column("fact")
        table.add_column("value")
        table.add_column("created")

        for f in item.facts:
            table.add_row(item.id, f.tx, f.get_key(), "" if f.value is None else str(f.value), f.created)

        print()
        print(table)


@dataclass
class Fact:
    id: str
    tag: str
    fact: str
    value: str
    tx: str
    created: str

    def is_tag(self):
        return self.fact is None

    def get_key(self):
        return f"{self.tag}{'/'+self.fact if self.fact is not None else ''}"

    def is_content(self):
        return self.tag == "db" and self.fact == "content"

    def as_string(self, markup=True):
        if self.is_content():
            return self.value

        output = f'[green][bold]#[/bold]{self.tag}[/green]' if markup else f'#{self.tag}'
        if self.fact:
            output += f'/[orange1]{self.fact}[/orange1]' if markup else f'/{self.fact}'
            if self.value is not None:
                output += f'=[yellow]{self.value}[/yellow]' if markup else f'={self.value}'

        return output


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
    "SET @1 #todo/completed",
    "SET @2 book appointment at physio",
    "GET @2",
    "HISTORY @2",
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
print('DATA')
pprint.pprint(DATA)

