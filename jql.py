#!/usr/bin/python3

import datetime

DATA = {}
TRANSACTIONS = {}


def new_transaction(query):
    new_id = len(TRANSACTIONS.keys())
    if TRANSACTIONS.get(new_id):
        raise Exception(f"{new_id} transaction should not already exist")

    tx = {"id": new_id, "timestamp": datetime.datetime.now(), "request": query, "user": "aaron", "client": "jql"}
    TRANSACTIONS[new_id] = tx
    return tx


def print_item(item):
    print(f"id: {item['db']['id']}")
    print(f"tags: {', '.join(item.keys())}")
    for tag in item.keys():
        if len(item[tag].keys()):
            for fact in item[tag].keys():
                print(f"#{tag}/{fact}={item[tag][fact]}")
        else:
            print(f"#{tag}")


def q(query):
    print(query)

    tokens = query.split(' ')

    action = tokens[0].lower()

    raw = []
    curr = []
    values = {"db": {}}
    for token in tokens[1:]:
        if token.startswith('#'):
            if curr:
                raw.append(' '.join(curr))
                curr = []
            raw.append(token)
        else:
            curr.append(token)
    if curr:
        raw.append(' '.join(curr))

    for r in raw:
        if not r.startswith('#'):
            values["db"]["content"] = r
            continue

        if "/" in r:
            tag, fact = r.split("/", 1)
        else:
            tag = r
            fact = None

        tag = tag.lstrip('#')
        if tag not in values.keys():
            values[tag] = {}

        if fact is not None:
            if '=' in fact:
                f, v = fact.split('=', 1)
                values[tag][f] = v
            else:
                values[tag][fact] = True


    #print(f'action: {action}')
    #print(f'raw: {raw}')
    #print(f'values: {values}')

    if action == 'create':
        tx = new_transaction(query)

        new_id = len(DATA.keys())
        if DATA.get(new_id):
            raise Exception(f"{new_id} should not already exist")

        if "id" in values["db"] or "tx" in values["db"]:
            raise Exception("Cannot hardcode db/tx or db/id")

        data = values.copy()
        data["db"]["id"] = new_id
        data["db"]["tx"] = tx.get('id')
        DATA[new_id] = data

        NO CREATE AS FACT TUPLES

        print_item(data)
        print()






q("CREATE go to supermarket #todo #todo/completed")
q("CREATE do dishes #todo #chores")
q("CREATE book appointment #todo #todo/remind_at=20210412")

import pprint
print('TRANSACTIONS')
pprint.pprint(TRANSACTIONS)
print('DATA')
pprint.pprint(DATA)

import sys
sys.exit()


from prompt_toolkit import print_formatted_text as print
from prompt_toolkit import PromptSession

session = PromptSession()

print('Welcome to JQL')
session.prompt()
