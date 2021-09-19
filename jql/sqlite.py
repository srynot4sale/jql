import sqlite3
import structlog
from typing import FrozenSet, List, Iterable, Set, Optional
import uuid


from jql.db import Store
from jql.types import Fact, Item, update_item, is_tag, is_flag, is_content, is_ref, has_value


log = structlog.get_logger()


class SqliteStore(Store):
    def __init__(self, location: str = ":memory:", salt: str = "") -> None:
        self._conn = sqlite3.connect(location)

        cur = self._conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", ["config"])
        if not cur.fetchone():
            cur.execute('''CREATE TABLE config
                        (key text, val text)''')
            cur.execute('''CREATE TABLE reflist
                        (created text, ref text)''')
            cur.execute('''CREATE TABLE facts
                        (created text, ref text, tag text, prop text, val text)''')
            cur.execute('''CREATE TABLE archived
                        (created text, ref text, tag text, prop text, val text)''')

            # Generate salt
            super().__init__(salt)

            cur.execute('INSERT INTO config (key, val) VALUES (?, ?)', ['salt', self._salt])
        else:
            # Load salt
            cur.execute("SELECT val FROM config WHERE key='salt'")
            loaded_salt = cur.fetchone()[0]
            super().__init__(loaded_salt)

        self._conn.commit()

    def _item_count(self) -> int:
        cur = self._conn.cursor()
        cur.execute('INSERT INTO reflist (ref) VALUES (?)', [str(uuid.uuid4())])
        itemid = int(cur.lastrowid)
        self._conn.commit()
        return itemid

    def next_ref(self) -> Fact:
        new_ref = super().next_ref()

        # Update row in reflist with generated hash
        cur = self._conn.cursor()
        cur.execute('UPDATE reflist SET ref = ? WHERE rowid = ?', (new_ref.value, self.ref_to_id(new_ref)))
        if cur.rowcount != 1:
            raise Exception(f"Unexpected result when storing new reference value '{new_ref.value}'")
        self._conn.commit()
        return new_ref

    def _get_item(self, ref: Fact) -> Optional[Item]:
        cur = self._conn.cursor()
        facts: Set[Fact] = set()
        for row in cur.execute('SELECT tag, prop, val FROM facts WHERE ref = ?', [ref.value]):
            facts.add(Fact(row[0], row[1], row[2]))
        if len(facts) == 0:
            return None
        return Item(facts=facts)

    def _get_items(self, search: Iterable[Fact]) -> List[Item]:
        matches = []

        # Generate where clause
        where = []
        d = []
        # For each item, loop through each search term
        s = 0
        for fact in search:
            s += 1
            prefix = f"f{s}"
            if is_tag(fact):
                w = f"{prefix}.tag = ?"
                d.append(fact.tag)
            elif is_flag(fact):
                w = f"{prefix}.tag = ? AND {prefix}.prop = ?"
                d.append(fact.tag)
                d.append(fact.prop)
            elif is_content(fact):
                # Content is a caseless substr match
                w = f"{prefix}.tag = 'db' AND {prefix}.prop = 'content' AND {prefix}.val LIKE ?"
                d.append(f'%{fact.value}%')
            elif not is_ref(fact) and has_value(fact):
                w = f"{prefix}.tag = ? AND {prefix}.prop = ? AND {prefix}.val = ?"
                d.append(fact.tag)
                d.append(fact.prop)
                d.append(fact.value)

            where.append(f" INNER JOIN facts AS {prefix} ON r.ref = {prefix}.ref AND {w} ")

        cur = self._conn.cursor()
        items_sql = '''
        SELECT ref, tag, prop, val FROM facts WHERE ref IN (
            SELECT DISTINCT r.ref
            FROM reflist AS r
        '''
        for w in where:
            items_sql += w

        items_sql += '''
            )
        ORDER BY rowid
        '''

        facts = {}  # type: ignore
        log.msg(items_sql)
        for row in cur.execute(items_sql, d):
            if row[0] not in facts.keys():
                facts[row[0]] = set()
            facts[row[0]].add(Fact(row[1], row[2], row[3]))

        for fs in facts.values():
            matches.append(Item(facts=fs))

        return matches

    def _create_item(self, item: Item) -> Item:
        self._add_facts(item.ref, item.facts)
        return item

    def _update_item(self, item: Item, new_facts: Set[Fact]) -> Item:
        self._add_facts(item.ref, frozenset(new_facts))
        updated_item = update_item(item, new_facts)
        return updated_item

    def _add_facts(self, ref: Fact, facts: FrozenSet[Fact]) -> None:
        cur = self._conn.cursor()
        for f in facts:
            cur.execute('INSERT INTO facts (ref, tag, prop, val) VALUES (?, ?, ?, ?)', (ref.value, f.tag, f.prop, f.value))
        self._conn.commit()
