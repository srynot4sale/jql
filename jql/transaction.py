from __future__ import annotations
import datetime
import lark.exceptions
import structlog
from typing import Iterable, List, Optional, Tuple, TYPE_CHECKING
import uuid

if TYPE_CHECKING:
    from jql.client import Client
    from jql.store import Store

from jql.parser import jql_parser, JqlTransformer
from jql.types import Item, Fact, Flag, is_ref, has_flag, Ref, Value
from jql.changeset import Change, ChangeSet


logger = structlog.get_logger()


class Transaction:
    def __init__(self, client: Client, store: Store):
        self.created: Optional[datetime.datetime] = None
        self._store = store
        self._client = client

        self.query: str = ''
        self.changeset: Optional[ChangeSet] = None
        self.response: List[Item] = []
        self.closed = False
        self.log = logger.bind()

    def __repr__(self) -> str:
        return f"Transaction({self.query})"

    def add_response(self, response: List[Item]) -> None:
        self.log.msg(response)
        self.response.extend(response)

    def commit(self) -> None:
        if self.changeset:
            self.log.msg("tx.commit()", changeset=self.changeset)
            cid = self._store.record_changeset(self.changeset)
            self.add_response(self._store.apply_changeset(cid))
            self.closed = True

    def start(self) -> None:
        if self.is_closed():
            raise Exception("Transaction already completed")
        if not self.created:
            self.created = datetime.datetime.now()

    def is_closed(self) -> bool:
        return self.closed is True

    def _add_change(self, change: Change) -> None:
        if not self.created:
            raise Exception("Transaction not started")

        if not self.changeset:
            self.changeset = ChangeSet(
                uuid=str(uuid.uuid4()),
                origin=self._store.uuid,
                origin_rowid=0,
                client=self._client.ref,
                created=self.created,
                query=self.query,
                changes=[]
            )

        self.changeset.changes.append(change)

    def create_item(self, facts: Iterable[Fact]) -> None:
        if not facts:
            raise Exception("No data supplied")
        facts = set(facts)
        self.start()
        self.log.msg("tx.create_item()", facts=facts)
        if not has_flag(Item(facts=facts), '_db', 'created'):
            facts.add(Value('_db', 'created', str(datetime.datetime.now())))

        self._add_change(Change(uuid=str(uuid.uuid4()), facts=facts))

    def revoke_facts(self, ref: Fact, facts: Iterable[Fact]) -> None:
        if not facts:
            raise Exception("No data supplied")
        facts = set(facts)
        self.start()
        self.log.msg("tx.revoke_facts()", ref=ref, facts=facts)
        uid = self._store._ref_to_uuid(ref)
        if not uid:
            raise Exception("Cannot find item")
        self._add_change(Change(uuid=uid, facts=facts, revoke=True))

    def set_facts(self, ref: Fact, facts: Iterable[Fact]) -> None:
        if not facts:
            raise Exception("No data supplied")
        facts = set(facts)
        self.start()
        self.log.msg("tx.set_facts()", ref=ref, facts=facts)
        uid = self._store._ref_to_uuid(ref)
        if not uid:
            raise Exception("Cannot find item")
        self._add_change(Change(uuid=uid, facts=facts))

    def get_item(self, ref: Fact) -> None:
        self.start()
        self.log.msg("tx.get_item()", ref=ref)
        self.add_response([self._get_item(ref)])

    def get_items(self, search: Iterable[Fact]) -> None:
        if not search:
            raise Exception("No search criteria supplied")
        self.start()
        self.log.msg("tx.get_items()", search=search)
        self.add_response(self._get_items(search))

    def get_history(self, search: Optional[Fact] = None) -> None:
        self.start()
        self.log.msg("tx.get_history()", search=search)
        self.add_response(self._store._get_history(search))

    def get_hints(self, search: str = '') -> None:
        self.start()
        # self.log.msg("tx.get_hints()", search=search)
        self.add_response(self._store.get_hints(search))

    def get_changesets(self) -> None:
        self.start()
        self.log.msg("tx.get_changesets()")
        self.add_response(self._store.get_changesets())

    def _get_item(self, ref: Fact) -> Item:
        if not is_ref(ref):
            raise Exception("Not a ref")
        item = self._store.get_item(ref)
        if not item:
            raise Exception(f'{ref} does not exist')
        return item

    def _get_items(self, search: Iterable[Fact]) -> List[Item]:
        return self._store.get_items(search)

    def trigger_replication(self) -> None:
        self.log.msg('tx.trigger_replication()')
        self._store.replicate_changesets()
        self._store.ingest_replication()

    def query_to_tree(self, query: str, log_errors: bool = True, replacements: Optional[List[Tuple[str, str]]] = None) -> Tuple[str, List[Fact]]:
        self.log = self.log.bind(query=query)
        try:
            tree = jql_parser.parse(query)
        except lark.exceptions.UnexpectedInput as e:
            err = str(e).splitlines()[0]
            if log_errors:
                self.log.error(err)
            raise Exception(f'Query error: {err}')

        ast = JqlTransformer().transform(tree)
        values: List[Fact] = [c for c in ast.children if isinstance(c, Fact)]

        # Replace any shortcuts
        if replacements:
            for s, ref in replacements:
                new_values = []
                for v in values:
                    if v == Ref(str(s)):
                        self.log.info(f'Replaced {s} with {ref}')
                        new_values.append(Ref(ref))
                    else:
                        new_values.append(v)
                values = new_values

        return (ast.data, values)

    def q(self, query: str, tree: Optional[Tuple[str, List[Fact]]] = None) -> List[Item]:
        self.start()
        self.query = query
        self.log.msg(f"Query '{query}'")

        if not tree:
            tree = self.query_to_tree(query)

        action, values = tree

        if action == 'create':
            self.create_item(values)
            self.commit()
            return self.response

        if action == 'archive':
            self.set_facts(values[0], [Flag('_db', 'archived')])
            self.commit()
            return self.response

        if action == 'set':
            self.set_facts(values[0], values[1:])
            self.commit()
            return self.response

        if action == 'del':
            self.revoke_facts(values[0], values[1:])
            self.commit()
            return self.response

        if action == 'get':
            self.get_item(values[0])
            return self.response

        if action == 'history':
            self.get_history(values[0] if values else None)
            return self.response

        if action == 'list':
            self.get_items(values)
            return self.response

        if action == 'hints':
            search = str(values[0]) if values else ''
            if search and self.query.endswith('/'):
                search += '/'
            self.get_hints(search)
            return self.response

        if action == 'changesets':
            self.get_changesets()
            return self.response

        if action == 'replicate':
            self.trigger_replication()
            return self.response

        raise Exception(f"Unknown query '{query}'")
