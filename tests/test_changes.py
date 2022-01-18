from jql.client import Client
from jql.memory import MemoryStore
from generator import yamltest


@yamltest
def test_basic_changesets_call(db: Client) -> str:
    return '''
    - q: "CREATE go to supermarket #todo"
      result:
        - key: a
          db:
            content: go to supermarket
          todo:
    - q: "@a SET #todo/completed"
      result:
        - key: b
          db:
            content: go to supermarket
          todo:
            completed:
    - q: "CHANGESETS"
      result:
        - db:
            content: '[{"facts": [{"tag": "todo", "prop": "completed", "value": "", "tx": null}], "ref": "??????", "uid": null, "revoke": false}]'
            tx:
          tx:
            created: ????-??-??
            query: '@a SET #todo/completed'
            uuid: ????????
            client: 'pytest:testuser'
    '''


@yamltest
def test_history(db: Client) -> str:
    if isinstance(db.store, MemoryStore):
        return ''

    return '''
    - q: "HISTORY"
      result:
    - q: "CREATE groceries #chores"
      result:
        - db:
            content: groceries
          chores:
    - q: "HISTORY"
      result:
        - db:
            content: "??????: Added Tag('chores')"
        - db:
            content: "??????: Added Content('groceries')"
        - db:
            content: "??????: Added Value(tag='db', prop='created', value='????-??-??')"
        - db:
            content: "??????: Added Ref('??????')"
    '''


@yamltest
def test_item_history(db: Client) -> str:
    if isinstance(db.store, MemoryStore):
        return ''

    return '''
    - q: "CREATE go to supermarket #todo"
      result:
        - key: a
          db:
            content: go to supermarket
          todo:
    - q: "@a SET #todo/completed"
      result:
        - db:
            content: go to supermarket
          todo:
            completed:
    - q: "@a HISTORY"
      result:
        - db:
            content: Added Content('go to supermarket')
        - db:
            content: Added Value(tag='db', prop='created', value='????-??-??')
        - db:
            content: Added Ref('??????')
        - db:
            content: Added Tag('todo')
        - db:
            content: Added Flag(tag='todo', prop='completed')
    '''
