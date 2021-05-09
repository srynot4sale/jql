#!./venv/bin/python3

from rich.console import Console
from rich.prompt import Prompt


from user import User


console = Console()
print = console.print


aaron = User("aaron")
client = aaron.get_client('jql')

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

