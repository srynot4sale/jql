import json
import sqlite3
import sys
from typing import Any, Dict, List

from jql.types import Ref
from jql.store.memory import MemoryStore


def schema_migration(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    current_version = cur.execute('pragma user_version').fetchone()[0]
    print(f'Current schema version: {current_version}')
    print('Run database migrations')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS
        config (
            key text,
            val text
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS
        idlist (
            ref text,
            uuid text,
            changeset_uuid text,
            created timestamp,
            archived timestamp
        )
    ''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_idlist_ref ON idlist (ref)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_idlist_created ON idlist (created)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_idlist_archived ON idlist (archived)''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS
        facts (
            changeset int,
            dbid int,
            tag text,
            prop text,
            val text,
            revoke int,
            current int
        )
    ''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_facts_dbid ON facts (dbid)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_facts_tag ON facts (tag)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_facts_prop ON facts (prop)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_facts_current ON facts (current)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_facts_revoke ON facts (revoke)''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS
        changesets (
            uuid text,
            client text,
            created timestamp,
            query text,
            changes text,
            origin text,
            origin_rowid int
        )
    ''')
    if current_version and current_version < 5:
        cur.execute('''ALTER TABLE changesets ADD COLUMN changes text''')

    if current_version and current_version < 10:
        cur.execute('''ALTER TABLE changesets ADD COLUMN origin text''')
        cur.execute('''ALTER TABLE changesets ADD COLUMN origin_rowid int''')

    cur.execute('''CREATE INDEX IF NOT EXISTS idx_changesets_uuid ON changesets (uuid)''')

    cur.execute('''
                CREATE VIEW IF NOT EXISTS items
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
                CREATE VIEW IF NOT EXISTS transactions
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
                CREATE VIEW IF NOT EXISTS current_facts_inc_archived
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
                CREATE TRIGGER IF NOT EXISTS archive_overriden_facts
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

    cur.execute('''PRAGMA user_version = 10''')

    conn.commit()


def data_migration(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # Check for required config vars
    config = {}
    for c in cur.execute("SELECT key, val FROM config"):
        config[c['key']] = c['val']

    print('Found config:')
    for c in config:
        print(f'\t{c}: {config[c]}')

    if 'salt' not in config or 'created' not in config:
        raise Exception('missing vital config')

    store = MemoryStore(salt=config['salt'])

    # Get idlist
    idlist: List[Dict[str, Any]] = []
    uids = {}
    changeset_uids = {}
    for i in cur.execute("SELECT rowid, * from idlist ORDER BY rowid ASC"):
        rowid = i['rowid']

        # Check
        if not len(i['ref']):
            raise Exception('missing ref', dict(i))

        if store.ref_to_id(Ref(i['ref'])) != rowid:
            raise Exception('ref does not map to rowid')

        if store.id_to_ref(rowid) != Ref(i['ref']):
            raise Exception('ref does not map to rowid')

        if not len(i['created']):
            raise Exception('missing created time', dict(i))

        if not i['uuid'] and not i['changeset_uuid']:
            raise Exception('should have at least one uuid', dict(i))

        if i['uuid']:
            uids[i['uuid']] = dict(i)
        elif i['changeset_uuid']:
            changeset_uids[i['changeset_uuid']] = dict(i)

        idlist.insert(rowid, dict(i))

    print(f'{len(idlist)} id\'s found')

    # Get changesets
    changesets: List[Dict[str, Any]] = []
    for c in cur.execute("SELECT rowid, * FROM changesets ORDER BY rowid ASC"):
        rowid = c['rowid']

        if not c['uuid']:
            raise Exception('should have an uuid', dict(c))

        if not c['client'] or ':' not in c['client']:
            raise Exception('shuold have a client defined', dict(c))

        if not c['created']:
            raise Exception('should have a created date assigned', dict(c))

        changesets.insert(rowid, dict(c))

    print(f'{len(changesets)} changesets found')

    try:
        # Review changesets
        for c in changesets:
            rowid = c['rowid']
            uid = c['uuid']
            created = c['created']
            client = c['client']
            origin = c['origin']
            if c['changes']:
                changes = json.loads(c['changes'])
            else:
                print('Loading changes from old changes table')
                # Get changes
                changes = []
                for ch in cur.execute("SELECT rowid, * FROM changes WHERE changeset = ? ORDER BY rowid ASC", [rowid]):
                    changes.append({
                        'ref': ch['ref'],
                        'uuid': ch['uuid'],
                        'revoke': ch['revoke'],
                        'facts': json.loads(ch['facts'])
                    })

                changes_json = json.dumps(changes)
                cur.execute('UPDATE changesets SET changes = ? WHERE rowid = ?', (changes_json, rowid))
            if not origin:
                print('Adding missing origin field to changeset')
                cur.execute('UPDATE changesets SET origin = ? WHERE rowid = ?', (config['salt'], rowid))
                origin = config['salt']

            # Get changeset item
            cidl = changeset_uids[uid]
            cidl_rowid = cidl['rowid']
            cidl_created = cidl['created']
            if cidl['uuid'] or not cidl['changeset_uuid']:
                raise Exception('Problem with changeset item!', dict(c), dict(cidl))

            if uid != cidl['changeset_uuid']:
                raise Exception('Problem with changeset item uuid!', dict(c), dict(cidl))

            if cidl_rowid != cidl['rowid']:
                raise Exception('Problem with loaded changeset item rowids!', dict(c), dict(cidl))

            if not cidl['created']:
                raise Exception('should have a created date assigned', dict(cidl))

            print(c)
            print(cidl)
            cs_facts = []
            csfs = [dict(csf) for csf in cur.execute("SELECT rowid, * FROM facts WHERE dbid = ? AND current = 1 ORDER BY rowid ASC", [cidl_rowid])]
            for csf in csfs:
                if csf['changeset'] != cidl_rowid:
                    print('Changeset items should refer to themselves as their changeset, fixing', csf)
                    csf['changeset'] = cidl_rowid
                    cur.execute('UPDATE facts SET changeset = ? WHERE rowid = ?', [csf['changeset'], csf['rowid']])

                if csf['tag'] == 'db':
                    csf['tag'] = '_db'
                    print('Updating to new style db tag', csf)
                    cur.execute('UPDATE facts SET tag = ?, prop = ? WHERE rowid = ?', [csf['tag'], csf['prop'], csf['rowid']])

                if csf['tag'] == '_db' or csf['tag'] == 'tx':
                    if csf['tag'] == '_db':
                        if csf['prop'] == 'txquery':
                            csf['tag'] = '_tx'
                            csf['prop'] = 'query'

                        if csf['prop'] == 'txcreated':
                            csf['tag'] = '_tx'
                            csf['prop'] = 'created'

                        if csf['prop'] == 'txclient':
                            csf['tag'] = '_tx'
                            csf['prop'] = 'client'

                        if csf['prop'] == 'tx':
                            csf['tag'] = '_tx'
                            csf['prop'] = ''
                    else:
                        csf['tag'] = '_tx'

                    if csf['tag'] == '_tx':
                        print('Updating to new style tx tag', csf)

                        cur.execute('UPDATE facts SET tag = ?, prop = ? WHERE rowid = ?', [csf['tag'], csf['prop'], csf['rowid']])

                cs_facts.append(csf)

            # Make sure it has the required facts
            found = False
            for csf in cs_facts:
                if csf['tag'] == '_db' and csf['prop'] == 'id':
                    found = True
                    if csf['val'] != cidl['ref']:
                        raise Exception('Incorrect _db/id set!', dict(csf), dict(cidl))
                    break

            if not found:
                print('No _db/id found, so inserting!', cs_facts)
                cur.execute('INSERT INTO facts (changeset, dbid, tag, prop, val, revoke, current) VALUES (?, ?, ?, ?, ?, ?, ?)', [cidl_rowid, cidl_rowid, '_db', 'id', cidl['ref'], 0, 1])

            found = False
            for csf in cs_facts:
                if csf['tag'] == '_db' and csf['prop'] == 'created':
                    found = True
                    break

            if not found:
                print('No _db/created found, so inserting!', cs_facts)
                cur.execute('INSERT INTO facts (changeset, dbid, tag, prop, val, revoke, current) VALUES (?, ?, ?, ?, ?, ?, ?)', [cidl_rowid, cidl_rowid, '_db', 'created', cidl_created, 0, 1])

            found = False
            for csf in cs_facts:
                if csf['tag'] == '_tx' and csf['prop'] == 'created':
                    found = True
                    if csf['val'] != created:
                        raise Exception('Wrong created time set for tx', dict(csf), dict(c))
                    break

            if not found:
                raise Exception('No _tx/created found!', cs_facts)

            found = False
            for csf in cs_facts:
                if csf['tag'] == '_tx' and csf['prop'] == 'client':
                    found = True
                    break

            if not found:
                print('No _tx/client found, so inserting!', cs_facts)
                cur.execute('INSERT INTO facts (changeset, dbid, tag, prop, val, revoke, current) VALUES (?, ?, ?, ?, ?, ?, ?)', [cidl_rowid, cidl_rowid, '_tx', 'client', client, 0, 1])

            found = False
            for csf in cs_facts:
                if csf['tag'] == '_tx' and csf['prop'] == '':
                    found = True
                    break

            if not found:
                raise Exception('No _tx tag found!', cs_facts)

            found = False
            for csf in cs_facts:
                if csf['tag'] == '_tx' and csf['prop'] == 'uuid':
                    found = True
                    if csf['val'] != uid:
                        raise Exception('Wrong uuid set for tx', dict(csf), dict(c))
                    break

            if not found:
                print('No _tx/uuid found, so inserting!', dict(cidl))
                cur.execute('INSERT INTO facts (changeset, dbid, tag, prop, val, revoke, current) VALUES (?, ?, ?, ?, ?, ?, ?)', [cidl_rowid, cidl_rowid, '_tx', 'uuid', uid, 0, 1])

            found = False
            for csf in cs_facts:
                if csf['tag'] == '_tx' and csf['prop'] == 'origin':
                    found = True
                    if csf['val'] != origin:
                        raise Exception('Wrong origin set for tx', dict(csf), dict(c))
                    break

            if not found:
                print('No _tx/origin found, so inserting!', cs_facts)
                cur.execute('INSERT INTO facts (changeset, dbid, tag, prop, val, revoke, current) VALUES (?, ?, ?, ?, ?, ?, ?)', [cidl_rowid, cidl_rowid, '_tx', 'origin', origin, 0, 1])

        # Review items
        for ui in uids:
            i = uids[ui]
            rowid = i['rowid']
            uid = i['uuid']
            created = i['created']
            archived = i['archived']

            if not i['created']:
                raise Exception('should have a created date assigned', dict(i))

            print(i)
            fs = [dict(f) for f in cur.execute("SELECT rowid, * FROM facts WHERE dbid = ? ORDER BY rowid ASC", [rowid])]
            for f in fs:

                if f['changeset'] == rowid:
                    raise Exception('Normal items should NOT refer to themselves as their changeset', i)

                if f['tag'] == 'db':
                    f['tag'] = '_db'
                    print('Updating to new style db tag', f)
                    cur.execute('UPDATE facts SET tag = ?, prop = ? WHERE rowid = ?', [f['tag'], f['prop'], f['rowid']])

                if f['tag'] == 'tx' or f['tag'] == '_tx':
                    raise Exception('Should not have a tx tag here!', i)

            facts = [dict(f) for f in cur.execute("SELECT rowid, * FROM facts WHERE dbid = ? ORDER BY rowid ASC", [rowid])]

            # Make sure it has the required facts
            found = False
            for csf in facts:
                if csf['tag'] == '_db' and csf['prop'] == 'id':
                    found = True
                    break

            if not found:
                raise Exception('no _db/id found!', facts)

            found = False
            for csf in facts:
                if csf['tag'] == '_db' and csf['prop'] == 'created':
                    found = True
                    break

            if not found:
                raise Exception('No _db/created found!', facts)

            found = False
            for csf in facts:
                if csf['tag'] == '_db' and csf['prop'] == 'archived':
                    found = True
                    break

            if bool(archived) != found:
                raise Exception('Archived flag doesn\'t match archived item state!', facts)

    except BaseException:
        raise

    finally:
        print('Comitting changes')
        conn.commit()


if __name__ == '__main__':
    dbpath = sys.argv[1]
    conn = sqlite3.connect(dbpath)
    schema_migration(conn)
    data_migration(conn)
