import os.path
from prompt_toolkit import PromptSession, HTML, print_formatted_text as print
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
import structlog


from jql.client import Client
from jql.memory import MemoryStore


log = structlog.get_logger()

store = MemoryStore()
store_path = "./mem.db"
if os.path.isfile(store_path):
    store.read_from_disk(open(store_path, mode="rb"))

client = Client(store=store, client="repl:user")

print('Welcome to JQL')
print('q to quit, h for help')

print(f"Logged in as {client.user}, with client {client.name}")

completer = WordCompleter(["CREATE", "SET", "GET", "HISTORY", "LIST", "QUIT"])
session: PromptSession[str] = PromptSession('> ', completer=completer, auto_suggest=AutoSuggestFromHistory())

while True:
    try:
        i = session.prompt()
        if i in ("q", "Q", "QUIT"):
            raise EOFError

        if i == "h":
            print('HELP!')
            continue

        tx = client.store.new_transaction()
        response = tx.q(i)

        if tx.changeset:
            print(HTML("<b>Changes:</b>"))
            for c in tx.changeset:
                print(f"  - {str(c)}")
            print()

        print(HTML("<b>Response:</b>"))
        for r in response:
            print(str(r))
        print()

    except KeyboardInterrupt:
        continue
    except EOFError:
        print('Quitting')
        store.persist_to_disk(open(store_path, mode="wb"))
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
