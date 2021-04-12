#!./venv/bin/python3

import datetime


from rich.console import Console
from rich.table import Table
from prompt_toolkit import print_formatted_text as print
from prompt_toolkit import PromptSession


DATA = {}
TRANSACTIONS = {}


def new_transaction(query):
    new_id = str(len(TRANSACTIONS.keys()))
    if TRANSACTIONS.get(new_id):
        raise Exception(f"{new_id} transaction should not already exist")

    tx = {"id": new_id, "timestamp": str(datetime.datetime.now().timestamp())[:-3], "request": query, "user": "aaron", "client": "jql"}
    TRANSACTIONS[new_id] = tx
    return tx


def new_item(tx):
    new_id = str(len(DATA.keys()))
    if DATA.get(new_id):
        raise Exception(f"{new_id} should not already exist")

    DATA[new_id] = []
    new_fact(new_id, 'db', None, None, tx)
    return new_id


def get_tags(id):
    facts = DATA[id]
    return set(f[0] for f in facts if f[1] is None)


def new_fact(id, tag, fact, value, tx):
    # If we are adding a fact, check if already has the tag set or not
    if fact is not None and tag not in get_tags(id):
        t = (tag, None, None, tx.get('id'), tx.get('timestamp'))
        DATA[id].append(t)

    f = (tag, fact, value, tx.get('id'), tx.get('timestamp'))
    DATA[id].append(f)


def print_item(id):
    facts = DATA[id]
    tags = get_tags(id)

    table = Table(title=f"id: {id} tags: {', '.join(tags)}")
    table.add_column("id")
    table.add_column("tx")
    table.add_column("fact")
    table.add_column("value")
    table.add_column("created")

    for f in facts:
        tag, fact, value, tx, created = f
        if fact:
            fact = f'#{tag}/{fact}'
        else:
            fact = f'#{tag}'

        table.add_row(id, tx, fact, "" if value is None else str(value), created)

    Console().print(table)


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



examples = [
    "CREATE go to supermarket #todo #todo/completed",
    "CREATE do dishes #todo #chores",
    "CREATE book appointment #todo #todo/remind_at=20210412",
    "SET @1 #todo/completed",
    "SET @2 book appointment at physio"
]

for ex in examples:
    q(ex)


session = PromptSession()

print('Welcome to JQL')
print('q to quit, h for help')
while True:
    i = session.prompt('> ')
    if i == "q":
        print('Quitting')
        break

    if i == "h":
        print('Examples:')
        for ex in examples:
            print(f"- {ex}")
        print()
        continue

    try:
        q(i)
    except BaseException as e:
        print(f"Error occured: [{e.__class__.__name__}] {e}")
        raise


import pprint
print('TRANSACTIONS')
pprint.pprint(TRANSACTIONS)
print('DATA')
pprint.pprint(DATA)

