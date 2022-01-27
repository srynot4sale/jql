import logging
from prompt_toolkit import PromptSession, print_formatted_text as print
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.formatted_text.html import HTML, html_escape as e
import re
import structlog
import sys
from typing import List, Optional, Tuple


from jql.client import Client
from jql.store.sqlite import SqliteStore
from jql.types import Item, get_content, get_props, get_ref, get_tags, has_ref, has_sys_tag, has_value, get_facts, single


if len(sys.argv) > 1:
    store_path = sys.argv[1]
else:
    store_path = "./repl.jdb"

store = SqliteStore(location=store_path)
client = Client(store=store, client="repl:user", log_level=logging.ERROR)


log = structlog.get_logger()


print('Welcome to JQL')
print('q to quit, h for help')

print(f"Logged in to '{store_path}' as {client.user}, with client {client.name}")


class JqlCompleter(Completer):
    actions = ["CREATE", "SET", "DEL", "HINTS", "HISTORY", "QUIT", "CHANGESETS"]
    _FIND_WORD_RE = re.compile(r"([a-zA-Z0-9_@#=\/]+)")

    def get_completions(self, document, complete_event):  # type: ignore
        word = document.get_word_before_cursor(WORD=False, pattern=self._FIND_WORD_RE)
        if word.startswith('#'):
            tx = client.new_transaction()
            query = f'HINTS {word}' if len(word) > 1 else 'HINTS'
            try:
                tree = tx.query_to_tree(query, log_errors=False)
            except BaseException:
                return

            response = tx.q(query, tree=tree)

            for r in response:
                # Ignore system tags/facts
                if not get_tags(r):
                    continue
                # Get non db/count fact
                c = single({f for f in get_facts(r) if not has_sys_tag(f)})
                yield Completion(str(c), start_position=-len(word))
        else:
            for ac in self.actions:
                if ac.startswith(word):
                    yield Completion(ac, start_position=-len(word))


session: PromptSession[str] = PromptSession('> ', completer=JqlCompleter(), auto_suggest=AutoSuggestFromHistory())

shortcuts: List[Tuple[str, str]] = []


def render_item(item: Item, shortcut: Optional[str] = None) -> HTML:
    output = ''

    if shortcut is not None:
        output += f' <grey>@{shortcut}</grey>'
    else:
        output += '   '

    if has_ref(item):
        output += f' <skyblue><b>{get_ref(item)}</b></skyblue>'

    content = get_content(item).value
    if len(content):
        output += f' {e(content)}'

    for p in get_tags(item):
        output += f' <green>{p}</green>'

    for p in get_props(item):
        output += f' <green>#{p.tag}</green>'
        output += f'/<orange>{p.prop}</orange>'
        if has_value(p):
            output += f'=<yellow>{e(p.value)}</yellow>'

    return HTML(output)


while True:
    try:
        i = session.prompt()
        if not len(i):
            continue

        if i in ("q", "Q", "QUIT"):
            raise EOFError

        if i == "h":
            print('HELP!')
            continue

        tx = client.new_transaction()
        tree = tx.query_to_tree(i, replacements=shortcuts)
        response = tx.q(i, tree=tree)

        if tx.changeset:
            print(HTML("<b>Changes:</b>"))
            for c in tx.changeset.changes:
                print(f"  - {str(c)}")
            print()

        print(HTML("<b>Response:</b>"))
        if not response:
            print(HTML(" <i>empty</i>"))
        else:
            # Reset shortcuts
            shortcuts = []
            for r in response:
                shortcut = None
                if has_ref(r):
                    ref = get_ref(r).value
                    for s, sref in shortcuts:
                        if sref == ref:
                            shortcut = s
                            break

                    slen = len(shortcuts)
                    if shortcut is None and slen < 10:
                        shortcuts.append((str(slen), ref))
                        shortcut = str(slen)

                print(render_item(r, shortcut))
        print()

    except KeyboardInterrupt:
        continue
    except EOFError:
        print('Quitting')
        break
    except BaseException as ex:
        log.exception("Error occured", exception=ex)
