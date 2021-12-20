import datetime
import json
import os
import sqlite3
from typing import FrozenSet, List, Iterable, Set, Optional, Tuple


from jql.changeset import Change, ChangeSet
from jql.db import Store
from jql.types import Fact, Flag, Item, Ref, Value, is_tag, is_flag, is_content, has_value, Tag, fact_from_dict


class SqliteStore(Store):
    def __init__(self, location: str = ":memory:", salt: str = "") -> None:
        self._conn = sqlite3.connect(location)

        if os.getenv("DEBUG") or os.getenv("FLASK_ENV") == "development":
            self._conn.set_trace_callback(print)

        cur = self._conn.cursor()
        current_version = cur.execute('pragma user_version').fetchone()[0]
        if not current_version:
            cur.execute('''CREATE TABLE config (key text, val text)''')
            cur.execute('''CREATE TABLE idlist (ref text, uuid text, changeset_uuid text)''')
            cur.execute('''CREATE TABLE facts (changeset int, dbid int, tag text, prop text, val text, revoke int, current int)''')
            cur.execute('''CREATE TABLE changesets (uuid text, client text, created timestamp, query text)''')
            cur.execute('''CREATE TABLE changes (changeset int, ref text, uuid text, facts text, revoke int)''')

            cur.execute('''CREATE INDEX idx_idlist_ref ON idlist (ref)''')
            cur.execute('''CREATE INDEX idx_facts_dbid ON facts (dbid)''')
            cur.execute('''CREATE INDEX idx_changesets_uuid ON changesets (uuid)''')
            cur.execute('''CREATE INDEX idx_changes_changeset ON changes (changeset)''')
            cur.execute('''CREATE INDEX idx_facts_tag ON facts (tag)''')
            cur.execute('''CREATE INDEX idx_facts_prop ON facts (prop)''')
            cur.execute('''CREATE INDEX idx_facts_current ON facts (current)''')

            # Set schema version
            cur.execute('''PRAGMA user_version = 1''')

        if current_version < 2:
            cur.execute('''CREATE INDEX idx_facts_revoke ON facts (revoke)''')
            cur.execute('''PRAGMA user_version = 2''')

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

    def _next_ref(self, uid: str, changeset: bool = False) -> Tuple[Fact, int]:
        cur = self._conn.cursor()

        if changeset:
            uids = [None, uid]
        else:
            uids = [uid, None]

        cur.execute('INSERT INTO idlist (uuid, changeset_uuid) VALUES (?, ?)', uids)
        itemid = int(cur.lastrowid)

        new_ref = self.id_to_ref(itemid)
        if self._get_item(new_ref):
            raise Exception(f"{new_ref} item should not already exist")

        if itemid != self.ref_to_id(new_ref):
            raise Exception("Ref and ID do not match")

        # Update row in reflist with generated hash
        if changeset:
            cur.execute('UPDATE idlist SET ref = ? WHERE rowid = ? AND uuid IS NULL AND changeset_uuid = ?', (new_ref.value, itemid, uids[1]))
        else:
            cur.execute('UPDATE idlist SET ref = ? WHERE rowid = ? AND uuid = ? AND changeset_uuid IS NULL', (new_ref.value, itemid, uids[0]))

        if cur.rowcount != 1:
            raise Exception(f"Unexpected result when storing new reference value '{new_ref.value}'")

        self._conn.commit()
        return (new_ref, itemid)

    def _get_item(self, ref: Fact) -> Optional[Item]:
        cur = self._conn.cursor()
        facts: Set[Fact] = set()
        for row in cur.execute('SELECT f.tag, f.prop, f.val FROM facts f INNER JOIN idlist i ON i.rowid = f.dbid WHERE f.current = 1 AND f.revoke = 0 AND i.ref = ?', [ref.value]):
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

            where.append(f" INNER JOIN facts AS {prefix} ON i.rowid = {prefix}.dbid AND {prefix}.current = 1 AND {prefix}.revoke = 0 AND {w} ")

        cur = self._conn.cursor()
        items_sql = '''
        SELECT dbid, tag, prop, val FROM facts WHERE dbid IN (
            SELECT DISTINCT i.rowid
            FROM idlist AS i
        '''
        for w in where:
            items_sql += w

        items_sql += '''
            AND i.changeset_uuid IS NULL
            ORDER BY i.rowid
            LIMIT 100
            )
        AND current = 1 AND revoke = 0
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
        self._add_facts(item.ref, item.facts, create=True)
        return item

    def _update_item(self, ref: Fact, new_facts: Set[Fact]) -> Item:
        self._add_facts(ref, frozenset(new_facts))
        updated_item = self._get_item(ref)
        if not updated_item:
            raise Exception("Updated item not found")
        return updated_item

    def _revoke_item_facts(self, ref: Fact, revoke: Set[Fact]) -> Item:
        self._add_facts(ref, frozenset(revoke), revoke=True)
        updated_item = self._get_item(ref)
        if not updated_item:
            raise Exception("Updated item not found")
        return updated_item

    def _add_facts(self, ref: Fact, facts: FrozenSet[Fact], revoke: bool = False, create: bool = False) -> None:
        cur = self._conn.cursor()
        dbid = cur.execute("SELECT rowid FROM idlist WHERE ref=?", (ref.value,)).fetchone()[0]
        values = []
        for f in facts:
            values.append((dbid, f.tag, f.prop, f.value, revoke))
        cur.executemany('INSERT INTO facts (dbid, tag, prop, val, revoke, current) VALUES (?, ?, ?, ?, ?, 1)', values)

        if not create:
            # Calculate no longer current facts
            cur.execute('''
                UPDATE facts
                SET current = 0
                WHERE dbid = ?
                AND rowid NOT IN (
                    SELECT MAX(rowid)
                    FROM facts
                    WHERE dbid = ?
                    GROUP BY tag, prop
                )
            ''', [dbid, dbid])

        self._conn.commit()

    def _get_tags_as_items(self, prefix: str = '') -> List[Item]:
        tags: List[Tuple[Fact, str]] = []

        cur = self._conn.cursor()
        tags_sql = '''
            SELECT f.tag, COUNT(DISTINCT f.dbid)
            FROM facts f
            INNER JOIN idlist i
            ON i.rowid = f.dbid
            AND i.changeset_uuid IS NULL
            WHERE f.revoke = 0 AND f.current = 1
        '''

        if len(prefix):
            tags_sql += ' AND f.tag LIKE ? '
            params = [f'{prefix}%']
        else:
            params = []

        tags_sql += '''
            GROUP BY f.tag
            ORDER BY f.tag
        '''

        for row in cur.execute(tags_sql, params):
            tags.append((Tag(row[0]), str(row[1])))

        return [Item(facts={t[0], Value('db', 'count', t[1])}) for t in tags]

    def _get_props_as_items(self, tag: str, prefix: str = '') -> List[Item]:
        props: List[Tuple[Fact, str]] = []

        cur = self._conn.cursor()
        props_sql = '''
            SELECT f.prop, COUNT(DISTINCT f.dbid)
            FROM facts f
            INNER JOIN idlist i
            ON i.rowid = f.dbid
            AND i.changeset_uuid IS NULL
            WHERE f.revoke = 0 AND f.current = 1
            AND f.tag = ? AND f.prop != ""
        '''

        if len(prefix):
            props_sql += ' AND f.prop LIKE ? '
            params = [tag, f'{prefix}%']
        else:
            params = [tag]

        props_sql += '''
            GROUP BY f.prop
            ORDER BY f.prop
        '''

        for row in cur.execute(props_sql, params):
            props.append((Flag(tag, row[0]), str(row[1])))

        return [Item(facts={t[0], Value('db', 'count', t[1])}) for t in props]

    def _record_changeset(self, changeset: ChangeSet) -> str:
        cur = self._conn.cursor()
        cur.execute('INSERT INTO changesets (uuid, client, created, query) VALUES (?, ?, ?, ?)', (changeset.uuid, changeset.client, changeset.created, changeset.query))
        changeset_id = int(cur.lastrowid)

        values = []
        for c in changeset.changes:
            ref = c.ref.value if c.ref else ''
            uid = c.uid if c.uid else ''
            facts = json.dumps([dict(f) for f in c.facts])
            values.append((changeset_id, ref, uid, facts, c.revoke))

        cur.executemany('INSERT INTO changes (changeset, ref, uuid, facts, revoke) VALUES (?, ?, ?, ?, ?)', values)
        self._conn.commit()

        return changeset.uuid

    def _load_changeset(self, changeset_uuid: str) -> ChangeSet:
        cur = self._conn.cursor()
        cur.execute('SELECT rowid, client, created, query FROM changesets WHERE uuid = ?', (changeset_uuid,))
        cs = cur.fetchone()
        if not cs:
            raise Exception(f'Could not find changeset {changeset_uuid}')

        changeset = ChangeSet(
            uuid=changeset_uuid,
            client=cs[1],
            created=cs[2],
            query=cs[3],
            changes=[]
        )

        for c in cur.execute('SELECT rowid, ref, uuid, revoke, facts FROM changes WHERE changeset = ?', (cs[0],)):
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

    def _get_changesets_as_items(self) -> List[Item]:
        cur = self._conn.cursor()

        cs_sql = '''
            SELECT f.dbid, f.tag, f.prop, f.val
            FROM facts f
            WHERE f.dbid IN (
                SELECT i.rowid
                FROM idlist i
                WHERE i.uuid IS NULL
                ORDER BY i.rowid DESC
                LIMIT 100
            )
            ORDER BY f.rowid DESC, f.dbid DESC
        '''

        sets: List[Item] = []
        facts = {}  # type: ignore
        for row in cur.execute(cs_sql):
            if row[0] not in facts.keys():
                facts[row[0]] = set()
            facts[row[0]].add(Fact(row[1], row[2], row[3]))

        for fs in facts.values():
            sets.append(Item(facts=fs))

        return sets
