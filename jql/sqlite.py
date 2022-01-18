import datetime
import json
import os
import sqlite3
from typing import FrozenSet, List, Iterable, Set, Optional, Tuple


from jql.changeset import Change, ChangeSet
from jql.store import Store
from jql.types import Content, Fact, Flag, Item, Ref, Value, is_tag, is_flag, is_content, get_ref, has_value, Tag, fact_from_dict


class SqliteStore(Store):
    def __init__(self, location: str = ":memory:", salt: str = "") -> None:
        self._conn = sqlite3.connect(location)

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

        if current_version < 3:
            cur.execute('''ALTER TABLE idlist ADD COLUMN created timestamp''')
            cur.execute('''ALTER TABLE idlist ADD COLUMN archived timestamp''')
            cur.execute('''CREATE INDEX idx_idlist_created ON idlist (created)''')
            cur.execute('''CREATE INDEX idx_idlist_archived ON idlist (archived)''')
            cur.execute('''UPDATE idlist SET archived = 0, created = date('now')''')
            cur.execute('''PRAGMA user_version = 3''')

        if current_version < 4:
            # Add some views to make queries easier to write
            cur.execute('''
                        CREATE VIEW current_facts
                        AS
                        SELECT i.ref, f.dbid, f.tag, f.prop, f.val, i.archived, i.created
                          FROM facts f
                         INNER JOIN idlist i
                            ON i.rowid = f.dbid
                         WHERE i.changeset_uuid IS NULL
                           AND f.current = 1
                           AND f.revoke = 0
            ''')
            cur.execute('''
                        CREATE VIEW current_items
                        AS
                        SELECT i.ref, f.dbid, f.tag, f.prop, f.val, i.created
                          FROM facts f
                         INNER JOIN idlist i
                            ON i.rowid = f.dbid
                         WHERE i.changeset_uuid IS NULL
                           AND i.archived = 0
                           AND f.current = 1
                           AND f.revoke = 0
            ''')

            cur.execute('''PRAGMA user_version = 4''')

        if current_version < 5:
            # Migrate changes into changeset table
            cur.execute('''ALTER TABLE changesets ADD COLUMN changes text''')

            for cs in cur.execute('SELECT rowid FROM changesets ORDER BY rowid'):
                # Get changes
                changes = []
                for c in cur.execute('SELECT rowid, ref, uuid, revoke, facts FROM changes WHERE changeset = ?', (cs[0],)):
                    changes.append({
                        'ref': c[1],
                        'uid': c[2],
                        'revoke': c[3],
                        'facts': json.loads(c[4])
                    })

                changes_json = json.dumps(changes)
                cur.execute('UPDATE changesets SET changes = ? WHERE rowid = ?', (changes_json, cs[0]))

            cur.execute('''DROP TABLE changes''')
            cur.execute('''PRAGMA user_version = 5''')

        if current_version < 6:
            # Add another view to make queries easier to write
            cur.execute('''
                        CREATE VIEW current_facts_inc_tx
                        AS
                        SELECT i.ref, f.dbid, f.tag, f.prop, f.val, i.archived, i.created
                          FROM facts f
                         INNER JOIN idlist i
                            ON i.rowid = f.dbid
                         WHERE f.current = 1
                           AND f.revoke = 0
            ''')

            cur.execute('''PRAGMA user_version = 6''')

        if current_version < 7:
            # Backfill missing created props
            # Removed due to incompatible code change
            cur.execute('''PRAGMA user_version = 7''')

        if current_version < 8:
            # View improvements
            cur.execute('''
                        CREATE VIEW items
                        AS
                        SELECT rowid, ref, uuid, archived, created
                          FROM idlist
                         WHERE changeset_uuid IS NULL
            ''')
            cur.execute('''DROP VIEW IF EXISTS current_items''')
            cur.execute('''
                        CREATE VIEW current_items
                        AS
                        SELECT rowid, ref, uuid, created
                          FROM items
                         WHERE archived = 0
            ''')
            cur.execute('''
                        CREATE VIEW transactions
                        AS
                        SELECT rowid, ref, changeset_uuid AS uuid, archived, created
                          FROM idlist
                         WHERE uuid IS NULL
            ''')
            cur.execute('''DROP VIEW IF EXISTS current_facts_inc_tx''')
            cur.execute('''
                        CREATE VIEW current_facts_inc_tx
                        AS
                        SELECT i.ref, f.dbid, f.tag, f.prop, f.val, t.ref as tx_ref, i.archived, i.created, CASE WHEN i.changeset_uuid IS NOT NULL THEN 1 ELSE 0 END AS is_tx
                          FROM facts f
                         INNER JOIN idlist i
                            ON i.rowid = f.dbid
                         INNER JOIN transactions t
                            ON t.rowid = f.changeset
                         WHERE f.current = 1
                           AND f.revoke = 0
            ''')
            cur.execute('''
                        CREATE VIEW current_facts_inc_archived
                        AS
                        SELECT ref, dbid, tag, prop, val, tx_ref, archived, created
                          FROM current_facts_inc_tx
                         WHERE is_tx = 0
            ''')
            cur.execute('''DROP VIEW IF EXISTS current_facts''')
            cur.execute('''
                        CREATE VIEW current_facts
                        AS
                        SELECT ref, dbid, tag, prop, val, tx_ref, created
                          FROM current_facts_inc_archived
                         WHERE archived = 0
            ''')
            cur.execute('''
                        CREATE TRIGGER archive_overriden_facts
                        AFTER INSERT
                         ON facts
                        FOR EACH ROW
                        WHEN
                          1 NOT IN (SELECT COUNT(rowid) FROM facts WHERE current = 1 AND dbid = new.dbid AND tag = new.tag AND prop = new.prop)
                        BEGIN
                            UPDATE facts
                            SET current = 0
                            WHERE dbid = new.dbid
                              AND rowid != new.rowid
                              AND current = 1
                              AND tag = new.tag
                              AND prop = new.prop;
                        END
            ''')

            cur.execute('''PRAGMA user_version = 8''')

        if current_version < 9:
            # Backfill missing changeset ids
            cur.execute('''UPDATE facts SET changeset = 1 WHERE changeset IS NULL''')
            cur.execute('''PRAGMA user_version = 9''')

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

        if os.getenv("DEBUG") or os.getenv("FLASK_ENV") == "development":
            self._conn.set_trace_callback(print)

    def _next_ref(self, uid: str, created: str, changeset: bool = False) -> Tuple[Fact, int]:
        cur = self._conn.cursor()

        if changeset:
            uids = [None, uid]
        else:
            uids = [uid, None]

        cur.execute('INSERT INTO idlist (created, uuid, changeset_uuid, archived) VALUES (?, ?, ?, 0)', [created, uids[0], uids[1]])
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
        for row in cur.execute('SELECT tag, prop, val, tx_ref FROM current_facts_inc_tx WHERE ref = ?', [ref.value]):
            facts.add(Fact(tag=row[0], prop=row[1], value=row[2], tx=row[3]))
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

            where.append(f" INNER JOIN current_facts AS {prefix} ON c.dbid = {prefix}.dbid AND {w} ")

        cur = self._conn.cursor()
        items_sql = '''
        SELECT c.dbid, c.tag, c.prop, c.val, c.tx_ref
        FROM current_facts c
        '''
        for w in where:
            items_sql += w

        items_sql += '''
        ORDER BY c.created
        '''

        facts = {}  # type: ignore
        for row in cur.execute(items_sql, d):
            if row[0] not in facts.keys():
                if len(facts) >= 100:
                    break
                facts[row[0]] = set()
            facts[row[0]].add(Fact(tag=row[1], prop=row[2], value=row[3], tx=row[4]))

        for fs in facts.values():
            matches.append(Item(facts=fs))

        return matches

    def _create_item(self, changeset_ref: Fact, item: Item) -> Item:
        self._add_facts(changeset_ref, get_ref(item), item.facts, create=True)
        return item

    def _update_item(self, changeset_ref: Fact, ref: Fact, new_facts: Set[Fact]) -> Item:
        self._add_facts(changeset_ref, ref, frozenset(new_facts))
        updated_item = self._get_item(ref)
        if not updated_item:
            raise Exception("Updated item not found")
        return updated_item

    def _revoke_item_facts(self, changeset_ref: Fact, ref: Fact, revoke: Set[Fact]) -> Item:
        self._add_facts(changeset_ref, ref, frozenset(revoke), revoke=True)
        updated_item = self._get_item(ref)
        if not updated_item:
            raise Exception("Updated item not found")
        return updated_item

    def _add_facts(self, changeset_ref: Fact, ref: Fact, facts: FrozenSet[Fact], revoke: bool = False, create: bool = False) -> None:
        cur = self._conn.cursor()
        dbid, archived = cur.execute("SELECT rowid, archived FROM idlist WHERE ref=?", (ref.value,)).fetchone()
        csid, = cur.execute("SELECT rowid FROM transactions WHERE ref=?", (changeset_ref.value,)).fetchone()
        values = []

        archive_changed = None
        for f in facts:
            if f.tag == "db" and f.prop == "archived":
                archive_changed = not revoke

            values.append((csid, dbid, f.tag, f.prop, f.value, revoke))

        cur.executemany('INSERT INTO facts (changeset, dbid, tag, prop, val, revoke, current) VALUES (?, ?, ?, ?, ?, ?, 1)', values)

        # Calculate if we need to change the archived state
        if archive_changed is not None and archive_changed != archived:
            cur.execute('UPDATE idlist SET archived = 1 WHERE ref = ?', (ref.value, ))

        self._conn.commit()

    def _get_tags_as_items(self, prefix: str = '') -> List[Item]:
        tags: List[Tuple[Fact, str]] = []

        cur = self._conn.cursor()
        tags_sql = '''
            SELECT tag, COUNT(DISTINCT dbid)
            FROM current_facts
        '''

        if len(prefix):
            tags_sql += ' WHERE tag LIKE ? '
            params = [f'{prefix}%']
        else:
            params = []

        tags_sql += '''
            GROUP BY tag
            ORDER BY tag
        '''

        for row in cur.execute(tags_sql, params):
            tags.append((Tag(row[0]), str(row[1])))

        return [Item(facts={t[0], Value('db', 'count', t[1])}) for t in tags]

    def _get_props_as_items(self, tag: str, prefix: str = '') -> List[Item]:
        props: List[Tuple[Fact, str]] = []

        cur = self._conn.cursor()
        props_sql = '''
            SELECT prop, COUNT(DISTINCT dbid)
            FROM current_facts
            WHERE tag = ? AND prop != ""
        '''

        if len(prefix):
            props_sql += ' AND prop LIKE ? '
            params = [tag, f'{prefix}%']
        else:
            params = [tag]

        props_sql += '''
            GROUP BY prop
            ORDER BY prop
        '''

        for row in cur.execute(props_sql, params):
            props.append((Flag(tag, row[0]), str(row[1])))

        return [Item(facts={t[0], Value('db', 'count', t[1])}) for t in props]

    def _record_changeset(self, changeset: ChangeSet) -> str:
        cur = self._conn.cursor()
        cur.execute('INSERT INTO changesets (uuid, client, created, query, changes) VALUES (?, ?, ?, ?, ?)', (changeset.uuid, changeset.client, changeset.created, changeset.query, json.dumps(changeset.changes_as_dict())))
        self._conn.commit()
        return changeset.uuid

    def _load_changeset(self, changeset_uuid: str) -> ChangeSet:
        cur = self._conn.cursor()
        cur.execute('SELECT rowid, client, created, query, changes FROM changesets WHERE uuid = ?', (changeset_uuid,))
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

        for c in json.loads(cs[4]):
            ref = Ref(c['ref']) if c['ref'] else None
            uid = c['uid'] if c['uid'] else None
            revoke = c['revoke']
            facts = {fact_from_dict(fact) for fact in c['facts']}

            changeset.changes.append(Change(
                ref=ref,
                uid=uid,
                facts=facts,
                revoke=revoke
            ))

        return changeset

    def _get_changesets_as_items(self) -> List[Item]:
        cur = self._conn.cursor()

        cs_sql = '''
            SELECT dbid, tag, prop, val
            FROM current_facts_inc_tx
            WHERE is_tx = 1
              AND dbid IN (
                SELECT rowid
                FROM transactions
                ORDER BY rowid DESC
                LIMIT 100
            )
            ORDER BY rowid DESC, dbid DESC
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

    def _get_history(self, ref: Optional[Fact] = None) -> List[Item]:
        cur = self._conn.cursor()

        cs_params = []
        cs_sql = '''
            SELECT i.ref, f.tag, f.prop, f.val, f.revoke, t.ref AS tx_ref, t.created AS tx_created
            FROM facts f
            INNER JOIN items i
               ON i.rowid = f.dbid
            INNER JOIN transactions t
               ON t.rowid = f.changeset
            WHERE
        '''

        if ref:
            cs_sql += '''
                i.ref = ?
            '''
            cs_params.append(ref.value)
        else:
            # Get last 100 transactions
            cs_sql += '''
                f.changeset IN (
                    SELECT rowid
                    FROM transactions
                    ORDER BY rowid DESC
                    LIMIT 100
                )
            '''

        cs_sql += '''
            AND f.dbid != f.changeset
            ORDER BY f.rowid DESC, f.dbid ASC, f.changeset DESC
        '''

        sets: List[Item] = []
        for row in cur.execute(cs_sql, cs_params):
            if not ref:
                desc = f'@{row[0]}: '
            else:
                desc = ''
            desc += 'Added ' if not row[4] else 'Revoked '
            desc += repr(Fact(row[1], row[2], row[3]))
            facts = {
                Ref(row[5]),
                Content(desc),
                Value('db', 'created', str(Ref(row[6])))
            }

            sets.append(Item(facts=facts))

        return sets
