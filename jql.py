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
            print('HELP!')
            continue

        tx = client.new_transaction()
        res = tx.q(i)
        tx.commit()

        res.print_item()
        print(f"Created @{res.id}")
        print()

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
