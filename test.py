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
