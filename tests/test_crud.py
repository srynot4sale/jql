from jql.store import Store
from generator import yamltest


@yamltest
def test_basic_create(db: Store) -> str:
    return '''
    - q: "CREATE go to supermarket #todo #todo/completed"
      result:
        - db:
            content: go to supermarket
          todo:
            completed:
    '''


@yamltest
def test_multiple_creates(db: Store) -> str:
    return '''
    - q: "CREATE do dishes #todo #chores"
      result:
        - db:
            content: do dishes
          todo:
          chores:
    - q: "CREATE groceries #chores"
      result:
        - db:
            content: groceries
          chores:
    '''


@yamltest
def test_create_archived(db: Store) -> str:
    return '''
    - q: "CREATE go to supermarket #todo #todo/completed #db/archived"
      result:
        - db:
            content: go to supermarket
            archived:
          todo:
            completed:
    - q: "#todo"
      result:
    '''


@yamltest
def test_basic_create_add_tags(db: Store) -> str:
    return '''
    - q: "CREATE do dishes #todo #chores"
      result:
        - key: a
          db:
            content: do dishes
          todo:
          chores:
    - q: "@a SET #new"
      result:
        - db:
            content: do dishes
          todo:
          chores:
          new:
    - q: "@a SET #another"
      result:
        - db:
            content: do dishes
          todo:
          chores:
          new:
          another:
    '''


@yamltest
def test_basic_add_remove_tags(db: Store) -> str:
    return '''
    - q: "CREATE do dishes #todo #chores"
      result:
        - key: a
          db:
            content: do dishes
          todo:
          chores:
    - q: "@a SET #new"
      result:
        - db:
            content: do dishes
          todo:
          chores:
          new:
    - q: "@a SET #another"
      result:
        - db:
            content: do dishes
          todo:
          chores:
          new:
          another:
    - q: "@a DEL #new"
      result:
        - db:
            content: do dishes
          todo:
          chores:
          another:
    - q: "@a SET #new"
      result:
        - db:
            content: do dishes
          todo:
          chores:
          new:
          another:
    - q: "@a DEL #another"
      result:
        - db:
            content: do dishes
          todo:
          chores:
          new:
    '''


@yamltest
def test_basic_create_add_facts(db: Store) -> str:
    return '''
    - q: "CREATE stuff #chores"
      result:
        - key: a
          db:
            content: stuff
          chores:
    - q: "@a SET #todo/immediately"
      result:
        - db:
            content: stuff
          chores:
          todo:
            immediately:
    - q: "@a SET #todo/nottomorrow"
      result:
        - db:
            content: stuff
          chores:
          todo:
            immediately:
            nottomorrow:
    '''


@yamltest
def test_basic_add_remove_facts(db: Store) -> str:
    return '''
    - q: "CREATE stuff #chores"
      result:
        - key: a
          db:
            content: stuff
          chores:
    - q: "@a SET #todo/immediately"
      result:
        - db:
            content: stuff
          chores:
          todo:
            immediately:
    - q: "@a SET #todo/nottomorrow"
      result:
        - db:
            content: stuff
          chores:
          todo:
            immediately:
            nottomorrow:
    - q: "@a DEL #todo/nottomorrow"
      result:
        - db:
            content: stuff
          chores:
          todo:
            immediately:
    - q: "@a DEL #todo"
      result:
        - db:
            content: stuff
          chores:
          todo:
            immediately:
    '''


@yamltest
def test_basic_tags_normalized(db: Store) -> str:
    return '''
    - q: "CREATE do dishes #todo #chores"
      result:
        - key: a
          db:
            content: do dishes
          todo:
          chores:
    - q: "@a SET #new"
      result:
        - db:
            content: do dishes
          todo:
          chores:
          new:
    - q: "@a SET #new"
      result:
        - db:
            content: do dishes
          todo:
          chores:
          new:
    '''


@yamltest
def test_list(db: Store) -> str:
    return '''
    - q: "CREATE do dishes #todo #chores"
      result:
        - key: a
          db:
            content: do dishes
          todo:
          chores:
    - q: "#chores"
      result:
        - key: a
    - q: "#todo"
      result:
        - key: a
    - q: "#notrealtag"
      result:
    - q: "do dishes"
      result:
        - key: a
    - q: "dish"
      result:
        - key: a
    - q: "disher"
      result:
    - q: "#todo #chores"
      result:
        - key: a
    - q: "#todo #fake"
      result:
    - q: "CREATE stuff #chores/late=yes"
      result:
        - key: b
          db:
            content: stuff
          chores:
            late: "yes"
    - q: "#todo"
      result:
        - key: a
    - q: "#chores"
      result:
        - key: a
        - key: b
    - q: "stuff"
      result:
        - key: b
    - q: "#chores/late"
      result:
        - key: b
    - q: "#chores/late=yes"
      result:
        - key: b
    - q: "#chores/late=no"
      result:
    '''


@yamltest
def test_list_by_content(db: Store) -> str:
    return '''
    - q: "CREATE do dishes for batman #todo #chores"
      result:
        - key: a
          db:
            content: do dishes for batman
          todo:
          chores:
    - q: "CREATE tears for bATman #chores/late=yes"
      result:
        - key: b
          db:
            content: tears for bATman
          chores:
            late: "yes"
    - q: "#chores"
      result:
        - key: a
        - key: b
    - q: "#todo"
      result:
        - key: a
    - q: "do dishes"
      result:
        - key: a
    - q: "dish"
      result:
        - key: a
    - q: "nopenope"
      result:
    - q: "for batman"
      result:
        - key: a
        - key: b
    - q: "for BATMAN"
      result:
        - key: a
        - key: b
    - q: "for"
      result:
        - key: a
        - key: b
    - q: "bat"
      result:
        - key: a
        - key: b
    - q: "MAN"
      result:
        - key: a
        - key: b
    '''


@yamltest
def test_list_with_revoke(db: Store) -> str:
    return '''
    - q: "CREATE do dishes #todo #chores"
      result:
        - key: a
          db:
            content: do dishes
          todo:
          chores:
    - q: "#chores"
      result:
        - key: a
    - q: "#notrealtag"
      result:
    - q: "@a DEL #chores"
      result:
        - key: b
          db:
            content: do dishes
          todo:
    - q: "#chores"
      result:
    - q: "#todo"
      result:
        - key: b
    '''


@yamltest
def test_list_with_archive(db: Store) -> str:
    return '''
    - q: "CREATE do dishes #todo #chores"
      result:
        - key: a
          db:
            content: do dishes
          todo:
          chores:
    - q: "#chores"
      result:
        - key: a
    - q: "#notrealtag"
      result:
    - q: "@a SET #db/archived"
      result:
        - key: b
          db:
            content: do dishes
            archived:
          todo:
          chores:
    - q: "#chores"
      result:
    - q: "#todo"
      result:
    '''
