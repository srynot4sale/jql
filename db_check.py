import sqlite3
import sys

from jql.types import Ref
from jql.memory import MemoryStore


dbpath = sys.argv[1]


conn = sqlite3.connect(dbpath)
conn.row_factory = sqlite3.Row

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
idlist = []
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

    idlist.insert(rowid, dict(i))

print(f'{len(idlist)} id\'s found')
