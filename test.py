#!/usr/bin/env python3

from rich.console import Console
from rich.prompt import Prompt


from user import User


console = Console()
print = console.print


examples = [
    "CREATE go to supermarket #todo #todo/completed",
    "CREATE do dishes #todo #chores",
    "CREATE book appointment #todo #todo/remind_at=20210412",
    "SET @2 #todo/completed",
    "SET @3 book appointment at physio",
    "GET @3",
    "HISTORY @3",
    "LIST #todo/completed",
    "LIST do dishes",
]

aaron = User("aaron", dsn="bolt://db:7687")
client = aaron.get_client('jql')

for ex in examples:
    print(ex)
    tx = client.new_transaction()
    tx.q(ex)
    tx.commit()
