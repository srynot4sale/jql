#!./venv/bin/python3

from rich.console import Console
from rich.prompt import Prompt


from user import User


console = Console()
print = console.print


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

