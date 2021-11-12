import datetime
import json
import sqlite3
from typing import FrozenSet, List, Iterable, Set, Optional, Tuple


from jql.changeset import Change, ChangeSet
from jql.db import Store
from jql.types import Fact, Flag, Item, Ref, Value, is_tag, is_flag, is_content, has_value, Tag, fact_from_dict


class SqliteStore(Store):
    def __init__(self, location: str = ":memory:", salt: str = "") -> None:
        self._conn = sqlite3.connect(location)

        cur = self._conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", ["config"])
        if not cur.fetchone():
            cur.execute('''CREATE TABLE config (key text, val text)''')
            cur.execute('''CREATE TABLE reflist (ref text, uuid text)''')
            cur.execute('''CREATE TABLE facts
                        (changeset int, ref text, tag text, prop text, val text)''')
            # cur.execute('''CREATE TABLE archived
            #            (ref text, tag text, prop text, val text)''')

        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", ["changesets"])
        if not cur.fetchone():
            cur.execute('''CREATE TABLE changesets
                        (client text, created timestamp, query text)''')
            cur.execute('''CREATE TABLE changes
                        (changeset int, ref text, uuid text, facts text, revoke int)''')

        # Look for existing salt
        cur.execute("SELECT val FROM config WHERE key='salt'")
        existing_salt = cur.fetchone()
        if existing_salt:
            super().__init__(existing_salt[0])
        else:
            # Run initial Setup with supplied salt (or generate one)
            super().__init__(salt)
            cur.execute('INSERT INTO config (key, val) VALUES (?, ?)', ['salt', self._salt])
            cur.execute('INSERT INTO config (key, val) VALUES (?, ?)', ['created', datetime.datetime.now()])

        self._conn.commit()

    def _next_ref(self, uid: str) -> Fact:
        cur = self._conn.cursor()

        cur.execute('INSERT INTO reflist (uuid) VALUES (?)', [uid])
        itemid = int(cur.lastrowid)

        new_ref = self.id_to_ref(itemid)
        if self._get_item(new_ref):
            raise Exception(f"{new_ref} item should not already exist")

        if itemid != self.ref_to_id(new_ref):
            raise Exception("Ref and ID do not match")

        # Update row in reflist with generated hash
        cur.execute('UPDATE reflist SET ref = ? WHERE rowid = ? AND uuid = ?', (new_ref.value, itemid, uid))
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
            elif has_value(fact):
                w = f"{prefix}.tag = ? AND {prefix}.prop = ? AND {prefix}.val = ?"
                d.append(fact.tag)
                d.append(fact.prop)
                d.append(fact.value)
            else:
                raise Exception(f'Unexpected search token {fact}')

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

    def _update_item(self, ref: Fact, new_facts: Set[Fact]) -> Item:
        self._add_facts(ref, frozenset(new_facts))
        updated_item = self._get_item(ref)
        if not updated_item:
            raise Exception("Updated item not found")
        return updated_item

    def _add_facts(self, ref: Fact, facts: FrozenSet[Fact]) -> None:
        values = []
        for f in facts:
            values.append((ref.value, f.tag, f.prop, f.value))
        cur = self._conn.cursor()
        cur.executemany('INSERT INTO facts (ref, tag, prop, val) VALUES (?, ?, ?, ?)', values)
        self._conn.commit()

    def _get_tags_as_items(self, prefix: str = '') -> List[Item]:
        tags: List[Tuple[Fact, str]] = []

        cur = self._conn.cursor()
        tags_sql = '''
            SELECT tag, COUNT(DISTINCT ref)
            FROM facts
            GROUP BY tag
            ORDER BY tag
        '''

        for row in cur.execute(tags_sql):
            if row[0].startswith(prefix):
                tags.append((Tag(row[0]), str(row[1])))

        return [Item(facts={t[0], Value('db', 'count', t[1])}) for t in tags]

    def _get_props_as_items(self, tag: str, prefix: str = '') -> List[Item]:
        tags: List[Tuple[Fact, str]] = []

        cur = self._conn.cursor()
        tags_sql = '''
            SELECT prop, COUNT(DISTINCT ref)
            FROM facts
            WHERE tag = ?
            GROUP BY prop
            ORDER BY prop
        '''

        for row in cur.execute(tags_sql, [tag]):
            if row[0] == "" and prefix == "":
                continue
            if row[0].startswith(prefix):
                tags.append((Flag(tag, row[0]), str(row[1])))

        return [Item(facts={t[0], Value('db', 'count', t[1])}) for t in tags]

    def _record_changeset(self, changeset: ChangeSet) -> int:
        cur = self._conn.cursor()
        cur.execute('INSERT INTO changesets (client, created, query) VALUES (?, ?, ?)', (changeset.client, changeset.created, changeset.query))
        changeset_id = int(cur.lastrowid)

        values = []
        for c in changeset.changes:
            ref = c.ref.value if c.ref else ''
            uid = c.uid if c.uid else ''
            facts = json.dumps([dict(f) for f in c.facts])
            values.append((changeset_id, ref, uid, facts, c.revoke))

        cur.executemany('INSERT INTO changes (changeset, ref, uuid, facts, revoke) VALUES (?, ?, ?, ?, ?)', values)
        self._conn.commit()

        return changeset_id

    def _load_changeset(self, changeset_id: int) -> ChangeSet:
        cur = self._conn.cursor()
        cur.execute('SELECT client, created, query FROM changesets WHERE rowid = ?', (changeset_id, ))
        cs = cur.fetchone()
        if not cs:
            raise Exception(f'Could not find changeset {changeset_id}')

        changeset = ChangeSet(
            client=cs[0],
            created=cs[1],
            query=cs[2],
            changes=[]
        )

        for c in cur.execute('SELECT rowid, ref, uuid, revoke, facts FROM changes WHERE changeset = ?', (changeset_id, )):
            ref = Ref(c[1]) if c[1] else None
            uid = c[2] if c[2] else None
            revoke = c[3]
            facts = set([fact_from_dict(fact) for fact in json.loads(c[4])])

            change = Change(
                ref=ref,
                uid=uid,
                facts=facts,
                revoke=revoke
            )

            changeset.changes.append(change)

        return changeset
