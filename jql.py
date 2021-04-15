#!./venv/bin/python3

from dataclasses import dataclass
import datetime


from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

console = Console()
print = console.print

DATA = {}
TRANSACTIONS = {}

@dataclass
class Transaction:
    id: str
    timestamp: str
    request: str
    user: str
    client: str


@dataclass
class Fact:
    id: str
    tag: str
    fact: str
    value: str
    tx: str
    created: str

    def get_key(self):
        return f"{self.tag}{'/'+self.fact if self.fact is not None else ''}"

    def is_content(self):
        return self.tag == "db" and self.fact == "content"

    def as_string(self):
        if self.is_content():
            return self.value
        if self.fact:
            if self.value is True:
                return f'#{self.tag}/{self.fact}'
            else:
                return f'#{self.tag}/{self.fact}={self.value}'
        else:
            return f'#{self.tag}'

    def as_markedup_string(self):
        if self.is_content():
            return self.as_string()
        if self.fact:
            if self.value is True:
                return f'[green][bold]#[/bold]{self.tag}[/green]/[orange1]{self.fact}[/orange1]'
            else:
                return f'[green][bold]#[/bold]{self.tag}[/green]/[orange1]{self.fact}[/orange1]=[yellow]{self.value}[/yellow]'
        else:
            return f'[green][bold]#[/bold]{self.tag}[/green]'

def new_transaction(query):
    new_id = str(len(TRANSACTIONS.keys()))
    if TRANSACTIONS.get(new_id):
        raise Exception(f"{new_id} transaction should not already exist")

    tx = Transaction(id=new_id, timestamp=str(datetime.datetime.now().timestamp())[:-3], request=query, user="aaron", client="jql")
    TRANSACTIONS[new_id] = tx
    return tx


def new_item(tx):
    new_id = str(len(DATA.keys()))
    if DATA.get(new_id):
        raise Exception(f"{new_id} should not already exist")

    DATA[new_id] = []
    new_fact(new_id, 'db', None, None, tx)
    return new_id


def get_tags(facts):
    return set(f.tag for f in facts if f.fact is None)


def new_fact(id, tag, fact, value, tx):
    # If we are adding a fact, check if already has the tag set or not
    if fact is not None and tag not in get_tags(get_item(id)):
        t = Fact(id=id, tag=tag, fact=None, value=None, tx=tx.id, created=tx.timestamp)
        DATA[id].append(t)

    f = Fact(id=id, tag=tag, fact=fact, value=value, tx=tx.id, created=tx.timestamp)
    DATA[id].append(f)


def summary_item(id):
    content = None
    facts = []
    for f in get_item(id):
        if f.is_content():
            content = f.as_string()
        else:
            facts.append(f.as_markedup_string())

    if content is not None:
        facts.insert(0, content)

    return f"[deep_sky_blue1]@{id}[/deep_sky_blue1] {' '.join(facts)}"


def get_item(id, history=False):
    all_facts = DATA[id]

    if history:
        facts = all_facts
    else:
        sorted_facts = {}
        for f in all_facts:
            if f.get_key() in sorted_facts.keys():
                # If existing tx is newer, don't update
                if sorted_facts[f.get_key()].tx > f.tx:
                    continue
            sorted_facts[f.get_key()] = f
        facts = sorted_facts.values()

    return facts


def print_item(id, history=False):
    facts = get_item(id, history)
    tags = get_tags(facts)

    table = Table(title=summary_item(id))
    table.add_column("id")
    table.add_column("tx")
    table.add_column("fact")
    table.add_column("value")
    table.add_column("created")

    for f in facts:
        table.add_row(id, f.tx, f.get_key(), "" if f.value is None else str(f.value), f.created)

    print()
    print(table)


def q(query):
    print(query)

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
            values.append((None, None, r.lstrip('@')))
            continue

        if not r.startswith('#'):
            values.append(("db", "content", r))
            continue

        if "/" in r:
            tag, fact = r.split("/", 1)
        else:
            tag = r
            fact = None

        tag = tag.lstrip('#')
        if fact is None:
            values.append((tag, None, None))
        else:
            if '=' in fact:
                f, v = fact.split('=', 1)
                values.append((tag, f, v))
            else:
                values.append((tag, fact, True))


    #print(f'action: {action}')
    #print(f'raw: {raw}')
    #print(f'values: {values}')

    if action == 'create':
        tx = new_transaction(query)
        id = new_item(tx)

        if not values:
            raise Exception("No data supplied")

        for tag, fact, value in values:
            if tag is None and fact is None:
                raise Exception("Not accepting ID's in a create")

            if tag == "db" and fact in ("id", "tx"):
                raise Exception("Cannot hardcode db/tx or db/id")

            new_fact(id, tag, fact, value, tx)

        print_item(id)
        print(f"Created @{id}")
        print()

    if action == 'set':
        tx = new_transaction(query)

        if len(values) < 2:
            raise Exception("No data supplied")

        if values[0][0] is not None and values[0][1] is not None:
            raise Exception(f"Expected an ID first - got {values[0]}")

        _, _, id = values[0]
        if id not in DATA.keys():
            raise Exception(f"Cannot find @{id}")

        for tag, fact, value in values[1:]:
            if tag == "db" and fact in ("id", "tx"):
                raise Exception("Cannot hardcode db/tx or db/id")

            new_fact(id, tag, fact, value, tx)

        print_item(id)
        print(f"Updated @{id}")
        print()

    if action == 'history':
        if not values:
            raise Exception("No data supplied")

        if values[0][0] is not None and values[0][1] is not None:
            raise Exception(f"Expected an ID first - got {values[0]}")

        _, _, id = values[0]
        if id not in DATA.keys():
            raise Exception(f"Cannot find @{id}")

        print_item(id, history=True)
        print()

    if action == 'get':
        if not values:
            raise Exception("No data supplied")

        if values[0][0] is not None and values[0][1] is not None:
            raise Exception(f"Expected an ID first - got {values[0]}")

        _, _, id = values[0]
        if id not in DATA.keys():
            raise Exception(f"Cannot find @{id}")

        print_item(id)
        print()

    if action == 'list':
        if not values:
            raise Exception("No data supplied")

        display = []
        for val in values:
            t, f, v = val
            if t == "db" and f == "content":
                display.append(v)
            if t is None:
                raise Exception("Can't list an ID")
            elif f is None:
                display.append(f'#{t}')
            elif v is None:
                display.append(f'#{t}/{f}')
            else:
                display.append(f'#{t}/{f}={v}')

        table = Table(title=f"List all items matching the search terms: {' '.join(display)}")
        table.add_column("item")

        # Check each data item as a current fact that matches every search term
        for id in DATA.keys():
            notfound = False
            facts = get_item(id)
            for val in values:
                if notfound:
                    break
                t, f, v = val
                match = False
                for fact in facts:
                    if t == fact.tag and f == fact.fact and v == fact.value:
                        match = True
                        #print(f"Match {val=} == {fact=}")
                        break
                    else:
                        #print(f"No match {val=} != {fact=}")
                        pass
                if not match:
                    notfound = True

            if not notfound:
                table.add_row(summary_item(id))

        print()
        print(table)
        print()



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

for ex in examples:
    q(ex)



print('Welcome to JQL')
print('q to quit, h for help')
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

        q(i)
    except BaseException as e:
        print(f"Error occured: [{e.__class__.__name__}] {e}")


import pprint
print('TRANSACTIONS')
pprint.pprint(TRANSACTIONS)
print('DATA')
pprint.pprint(DATA)

