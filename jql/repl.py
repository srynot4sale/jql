from prompt_toolkit import PromptSession, HTML, print_formatted_text as print
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
import structlog
from typing import List, Tuple


from jql.client import Client
from jql.sqlite import SqliteStore


log = structlog.get_logger()

store_path = "./repl.db"
store = SqliteStore(location=store_path)
client = Client(store=store, client="repl:user")

print('Welcome to JQL')
print('q to quit, h for help')

print(f"Logged in as {client.user}, with client {client.name}")

completer = WordCompleter(["CREATE", "SET", "GET", "HISTORY", "LIST", "QUIT"])
session: PromptSession[str] = PromptSession('> ', completer=completer, auto_suggest=AutoSuggestFromHistory())

shortcuts: List[Tuple[int, str]] = []

while True:
    try:
        i = session.prompt()
        if i in ("q", "Q", "QUIT"):
            raise EOFError

        if i == "h":
            print('HELP!')
            continue

        # Replace any shortcuts
        for s, ref in shortcuts:
            if f'@{s}' in i:
                i = i.replace(f'@{s}', f'@{ref}')
                print(HTML(f"<i>Replacing shortcut @{s} with @{ref}</i>"))

        tx = client.store.new_transaction()
        response = tx.q(i)

        if tx.changeset:
            print(HTML("<b>Changes:</b>"))
            for c in tx.changeset:
                print(f"  - {str(c)}")
            print()

        print(HTML("<b>Response:</b>"))
        if not response:
            print(HTML(" <i>empty</i>"))
        else:
            # Reset shortcuts
            shortcuts = []
            for r in response:
                shortcut = '  '
                s = len(shortcuts)
                if s < 10:
                    ref = r.ref.value
                    shortcuts.append((s, ref))
                    shortcut = f'<u>@{s}</u>'

                print(HTML(f" {shortcut} {r}"))
        print()

    except KeyboardInterrupt:
        continue
    except EOFError:
        print('Quitting')
        break
    except BaseException:
        log.exception("Error occured")

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
