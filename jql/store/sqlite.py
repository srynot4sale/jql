import datetime
import json
import os
import sqlite3
from typing import FrozenSet, List, Iterable, Set, Optional, Tuple


from jql.changeset import ChangeSet
from jql.store import Store
from jql.types import Content, Fact, Flag, Item, Ref, Value, is_tag, is_flag, is_content, has_value, Tag


class SqliteStore(Store):
    def __init__(self, location: str = ":memory:", salt: str = "") -> None:
        self._conn = sqlite3.connect(location)
        self._conn.row_factory = sqlite3.Row

        cur = self._conn.cursor()
        current_version = cur.execute('pragma user_version').fetchone()[0]
        if not current_version:
            import jql.store.sqlite_migration
            jql.store.sqlite_migration.schema_migration(self._conn)
        elif current_version < 11:
            raise Exception('Database needs migration run')

        # Look for existing salt
        cur.execute("SELECT val FROM config WHERE key='salt'")
        existing_salt = cur.fetchone()
        if existing_salt:
            super().__init__(existing_salt["val"])
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

        new_ref = self._id_to_ref(itemid)
        if self._get_item(new_ref):
            raise Exception(f"{new_ref} item should not already exist")

        if itemid != self._ref_to_id(new_ref):
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
            facts.add(self._fact_from_row(row))
        if len(facts) == 0:
            return None
        return Item(facts=facts)

    def _uuid_to_ref(self, uuid: str) -> Optional[Fact]:
        ref = self._conn.execute("SELECT ref FROM idlist WHERE uuid=?", (uuid,)).fetchone()
        return Ref(ref['ref']) if ref else None

    def _ref_to_uuid(self, ref: Fact) -> Optional[str]:
        uuid = self._conn.execute("SELECT uuid FROM idlist WHERE ref=?", (ref.value,)).fetchone()
        return uuid['uuid'] if uuid else None

    def _get_item_by_uuid(self, uuid: str) -> Optional[Item]:
        ref = self._uuid_to_ref(uuid)
        return self._get_item(ref) if ref else None

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
                w = f"{prefix}.tag = '_db' AND {prefix}.prop = 'content' AND {prefix}.val LIKE ?"
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
        SELECT c.dbid AS dbid, c.tag AS tag, c.prop AS prop, c.val AS val, c.tx_ref AS tx_ref
        FROM current_facts c
        '''
        for w in where:
            items_sql += w

        items_sql += '''
        ORDER BY c.created
        '''

        facts = {}  # type: ignore
        for row in cur.execute(items_sql, d):
            if row["dbid"] not in facts.keys():
                if len(facts) >= 100:
                    break
                facts[row["dbid"]] = set()
            facts[row["dbid"]].add(self._fact_from_row(row))

        for fs in facts.values():
            matches.append(Item(facts=fs))

        return matches

    def _create_item(self, changeset_ref: Fact, uid: str, item: Item) -> Item:
        self._add_facts(changeset_ref, uid, item.facts, create=True)
        return item

    def _update_item(self, changeset_ref: Fact, uid: str, new_facts: Set[Fact]) -> Item:
        self._add_facts(changeset_ref, uid, frozenset(new_facts))
        updated_item = self._get_item_by_uuid(uid)
        if not updated_item:
            raise Exception("Updated item not found")
        return updated_item

    def _revoke_item_facts(self, changeset_ref: Fact, uid: str, revoke: Set[Fact]) -> Item:
        self._add_facts(changeset_ref, uid, frozenset(revoke), revoke=True)
        updated_item = self._get_item_by_uuid(uid)
        if not updated_item:
            raise Exception("Updated item not found")
        return updated_item

    def _add_facts(self, changeset_ref: Fact, uid: str, facts: FrozenSet[Fact], revoke: bool = False, create: bool = False) -> None:
        cur = self._conn.cursor()
        res = cur.execute("SELECT rowid, archived FROM idlist WHERE uuid=? OR changeset_uuid=?", (uid, uid)).fetchone()
        if not res:
            raise Exception(f'Could not find item {uid} to update')
        dbid = res['rowid']
        archived = res['archived']

        res = cur.execute("SELECT rowid FROM transactions WHERE ref=?", (changeset_ref.value,)).fetchone()
        if not res:
            raise Exception('Could not find transaction')
        csid = res['rowid']

        values = []

        archive_changed = None
        for f in facts:
            if f.tag == "_db" and f.prop == "archived":
                archive_changed = not revoke

            values.append((csid, dbid, f.tag, f.prop, f.value, revoke))

        cur.executemany('INSERT INTO facts (changeset, dbid, tag, prop, val, revoke, current) VALUES (?, ?, ?, ?, ?, ?, 1)', values)

        # Calculate if we need to change the archived state
        if archive_changed is not None and archive_changed != archived:
            cur.execute('UPDATE idlist SET archived = 1 WHERE uuid = ?', (uid, ))

        self._conn.commit()

    def _get_tags_as_items(self, prefix: str = '') -> List[Item]:
        tags: List[Item] = []

        cur = self._conn.cursor()
        tags_sql = '''
            SELECT tag, COUNT(DISTINCT dbid) AS c
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
            tags.append(Item(facts={Tag(row["tag"]), Value('_db', 'count', str(row["c"]))}))
        return tags

    def _get_props_as_items(self, tag: str, prefix: str = '') -> List[Item]:
        props: List[Item] = []

        cur = self._conn.cursor()
        props_sql = '''
            SELECT prop, COUNT(DISTINCT dbid) AS c
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
            props.append(Item(facts={Flag(tag, row["prop"]), Value('_db', 'count', str(row["c"]))}))
        return props

    def _record_changeset(self, changeset: ChangeSet) -> str:
        cur = self._conn.cursor()
        cur.execute('INSERT INTO changesets (uuid, client, created, query, changes, origin, origin_rowid) VALUES (?, ?, ?, ?, ?, ?, ?)', (changeset.uuid, changeset.client, changeset.created, changeset.query, json.dumps(changeset.changes_as_dict()), changeset.origin, changeset.origin_rowid))
        self._conn.commit()
        return changeset.uuid

    def _load_changeset(self, changeset_uuid: str) -> ChangeSet:
        cur = self._conn.cursor()
        cur.execute('SELECT rowid, uuid, client, created, query, changes, origin, origin_rowid, applied, replicated FROM changesets WHERE uuid = ?', (changeset_uuid,))
        cs = cur.fetchone()
        if not cs:
            raise KeyError(f'Could not find changeset {changeset_uuid}')
        return self._changset_from_row(cs)

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
            if row["dbid"] not in facts.keys():
                facts[row["dbid"]] = set()
            facts[row["dbid"]].add(self._fact_from_row(row))

        for fs in facts.values():
            sets.append(Item(facts=fs))

        return sets

    def _get_history(self, ref: Optional[Fact] = None) -> List[Item]:
        cur = self._conn.cursor()

        cs_params = []
        cs_sql = '''
            SELECT i.ref AS ref, f.tag AS tag, f.prop AS prop, f.val AS val, f.revoke AS revoke, t.ref AS tx_ref, t.created AS tx_created
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
            ORDER BY f.tag ASC, f.prop ASC, f.val ASC, f.dbid ASC, f.changeset DESC
        '''

        sets: List[Item] = []
        for row in cur.execute(cs_sql, cs_params):
            if not ref:
                desc = f'@{row["ref"]}: '
            else:
                desc = ''
            desc += 'Added ' if not row["revoke"] else 'Revoked '
            desc += repr(self._fact_from_row(row))
            facts = {
                Ref(row["tx_ref"]),
                Content(desc),
                Value('_db', 'created', row["tx_created"])
            }

            sets.append(Item(facts=facts))

        return sets

    def _get_last_ingested_changeset(self, dbuuid: str) -> int:
        cur = self._conn.cursor()
        cs = cur.execute('SELECT MAX(origin_rowid) AS max FROM changesets WHERE origin = ? GROUP BY origin', (dbuuid,)).fetchone()
        return 0 if not cs else int(cs["max"])

    def _get_unreplicated_changesets(self) -> List[ChangeSet]:
        cur = self._conn.cursor()
        res = cur.execute('SELECT uuid, client, created, query, changes, origin, rowid AS origin_rowid, applied, replicated FROM changesets WHERE origin = ? AND applied = 1 AND (replicated = 0 OR replicated IS NULL) ORDER BY rowid', (self.uuid,))
        changesets = []
        for row in res:
            changesets.append(self._changset_from_row(row))
        return changesets

    def _changset_from_row(self, row: sqlite3.Row) -> ChangeSet:
        rowid = row['origin_rowid']
        if not rowid and row['origin'] == self.uuid:
            rowid = row['rowid']

        changeset = ChangeSet(
            uuid=row["uuid"],
            client=row["client"],
            created=datetime.datetime.fromisoformat(row['created']),
            query=row["query"],
            origin=row["origin"],
            origin_rowid=rowid,
            applied=bool(row["applied"]),
            replicated=bool(row["replicated"]),
            changes=ChangeSet.changes_from_dict(json.loads(row["changes"]))
        )
        return changeset

    def _fact_from_row(self, row: sqlite3.Row) -> Fact:
        return Fact(tag=row["tag"], prop=row["prop"], value=row["val"], tx=row["tx_ref"] if "tx_ref" in row else None)

    def _update_changeset(self, changeset: ChangeSet, replicated: Optional[bool] = None, applied: Optional[bool] = None) -> None:
        cur = self._conn.cursor()
        if replicated is not None:
            cur.execute('UPDATE changesets SET replicated = ? WHERE uuid = ?', (int(replicated), changeset.uuid))
            if cur.rowcount != 1:
                raise Exception(f"Unexpected result when updating changeset '{changeset.uuid}'")
        if applied is not None:
            cur.execute('UPDATE changesets SET applied = ? WHERE uuid = ?', (int(applied), changeset.uuid))
            if cur.rowcount != 1:
                raise Exception(f"Unexpected result when updating changeset '{changeset.uuid}'")
        self._conn.commit()
