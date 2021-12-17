from jql.db import Store
from generator import yamltest


interface = "query"


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
def test_basic_create_add_tags(db: Store) -> str:
    return '''
    - q: "CREATE do dishes #todo #chores"
      ref_alias: "a"
      result:
        - db:
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
def test_basic_create_add_facts(db: Store) -> str:
    return '''
    - q: "CREATE stuff #chores"
      ref_alias: "a"
      result:
        - db:
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
def test_basic_tags_normalized(db: Store) -> str:
    return '''
    - q: "CREATE do dishes #todo #chores"
      ref_alias: "a"
      result:
        - db:
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
      ref_alias: "a"
      ref:
        db:
          content: do dishes
        todo:
        chores:
      result:
        - "a"
    - q: "#chores"
      result:
        - "a"
    - q: "#todo"
      result:
        - "a"
    - q: "#notrealtag"
      result:
    - q: "do dishes"
      result:
        - "a"
    - q: "dish"
      result:
        - "a"
    - q: "disher"
      result:
    - q: "#todo #chores"
      result:
        - "a"
    - q: "#todo #fake"
      result:
    - q: "CREATE stuff #chores/late=yes"
      ref_alias: "b"
      ref:
        db:
          content: stuff
        chores:
          late: "yes"
      result:
        - "b"
    - q: "#todo"
      result:
        - "a"
    - q: "#chores"
      result:
        - "a"
        - "b"
    - q: "stuff"
      result:
        - "b"
    - q: "#chores/late"
      result:
        - "b"
    - q: "#chores/late=yes"
      result:
        - "b"
    - q: "#chores/late=no"
      result:
    '''


@yamltest
def test_list_by_content(db: Store) -> str:
    return '''
    - q: "CREATE do dishes for batman #todo #chores"
      ref_alias: "a"
      ref:
        db:
          content: do dishes for batman
        todo:
        chores:
      result:
        - "a"
    - q: "CREATE tears for bATman #chores/late=yes"
      ref_alias: "b"
      ref:
        db:
          content: tears for bATman
        chores:
          late: "yes"
      result:
        - "b"
    - q: "#chores"
      result:
        - "a"
        - "b"
    - q: "#todo"
      result:
        - "a"
    - q: "do dishes"
      result:
        - "a"
    - q: "dish"
      result:
        - "a"
    - q: "nopenope"
      result:
    - q: "for batman"
      result:
        - "a"
        - "b"
    - q: "for BATMAN"
      result:
        - "a"
        - "b"
    - q: "for"
      result:
        - "a"
        - "b"
    - q: "bat"
      result:
        - "a"
        - "b"
    - q: "MAN"
      result:
        - "a"
        - "b"
    '''
