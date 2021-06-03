#!./venv/bin/python3

from rich.console import Console
from rich.prompt import Prompt


from jql.memory import MemoryStore


console = Console()
cprint = console.print

store = MemoryStore()
user = store.get_user("repl")
client = user.get_client("jql")

cprint('Welcome to JQL')
cprint('q to quit, h for help')

cprint(f"Logged in as {user.name}, with client {client.name} at {client.tx}")

while True:
    try:
        i = Prompt.ask('')
        if i == "q":
            cprint('Quitting')
            break

        if i == "h":
            cprint('HELP!')
            continue

        tx = client.new_transaction(i)

        cprint("Changes:")
        for c in tx.tx.changeset:
            cprint(c)
        cprint()

        cprint("Response:")
        cprint(tx.response.to_dict())
        cprint()

    except BaseException as e:
        print(f"Error occured: [{e.__class__.__name__}] {e}")

"""
    def as_string(self, markup=True):
        if self.is_content():
            return self.value

        output = f'[green][bold]#[/bold]{self.tag}[/green]' if markup else f'#{self.tag}'
        if self.is_fact():
            output += f'/[orange1]{self.fact}[/orange1]' if markup else f'/{self.fact}'
            if self.has_value():
                output += f'=[yellow]{self.value}[/yellow]' if markup else f'={self.value}'

        return output
"""
