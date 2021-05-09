from rich.table import Table
from rich.console import Console

from fact import Fact


console = Console()
print = console.print

class Item:
    def __init__(self, tx, id, exists=False):
        self.tx = tx
        self.id = id
        self._facts = []
        self._current = []
        if exists:
            result = self.tx.get_one(f"MATCH (a:db) WHERE id(a) = $id RETURN a", {"id": int(self.id)})
            if not result:
                raise Exception(f'@{self.id} does not exist')
            else:
                self._set_facts(result[0])

    def __repr__(self):
        f = ',\n\t\t'.join([str(f) for f in self.get_facts(history=True)])
        return f"Item(\n\tid={self.id},\n\tfacts=[\n\t\t{f}\n\t])"

    def set_content(self, tx, content):
        self.add_fact(tx, 'db', 'content', content)

    def get_facts(self, history=False):
        self._facts = []
        result = self.tx.get_one(f"MATCH (a:db) WHERE id(a) = $id RETURN a", {"id": int(self.id)})
        #print(result)
        #print(result[0].labels)
        self._set_facts(result[0])
        return self._facts#if history else self._current

    def _set_facts(self, facts):
        for tag in facts.labels:
            self._facts.append(Fact(id=self.id, tag=tag, fact=None, value=None, tx="", created=""))

        for prop, val in facts.items():
            tag, fact = prop.split('_', 1)
            self._facts.append(Fact(id=self.id, tag=tag, fact=fact, value=val if val != True else None, tx="", created=""))

    def _save_fact(self, f):

        if f.fact is None:
            result = self.tx.run(f"MATCH (a:db) WHERE id(a) = $id SET a{f.db_key}", {"id": int(self.id)})
        elif f.value is None:
            result = self.tx.run(f"MATCH (a:db) WHERE id(a) = $id SET a{f.db_key} = true", {"id": int(self.id)})
        else:
            result = self.tx.run(f"MATCH (a:db) WHERE id(a) = $id SET a{f.db_key} = $val", {"id": int(self.id), "val": f.value})

    def add_tag(self, tx, tag):
        t = Fact(id=self.id, tag=tag, fact=None, value=None, tx=tx.id, created=tx.timestamp)
        self._save_fact(t)

    def add_fact(self, tx, tag, fact=None, value=None, special=False):
        if tag == 'db' and fact == 'id' and not special:
            raise Exception("Cannot change fact #db/id")

        # If we are adding a fact, check if already has the tag set or not
        if fact is not None and tag not in self.get_tags():
            self.add_tag(tx, tag)

        f = Fact(id=self.id, tag=tag, fact=fact, value=value, tx=tx.id, created=tx.timestamp)
        self._save_fact(f)

    def get_tags(self):
        return set(f.tag for f in self.get_facts() if f.is_tag())

    def summary(self, markup=True):
        content = None
        facts = []
        for f in self.get_facts():
            if f.tag == "db" and (f.fact is None or f.fact == "id"):
                continue
            if f.is_content():
                content = f.as_string(markup=markup)
            else:
                facts.append(f.as_string(markup=markup))

        if content is not None:
            facts.insert(0, content)

        if markup:
            return f"[deep_sky_blue1][bold]@[/bold]{self.id}[/deep_sky_blue1] {' '.join(facts)}"
        else:
            return f"@{self.id} {' '.join(facts)}"

    def print_item(self, history=False):
        table = Table(title=self.summary())
        table.add_column("fact")
        table.add_column("value")
        table.add_column("tx")
        table.add_column("created")

        for f in self.get_facts(history=history):
            table.add_row(f.get_key(), "" if f.value is None else str(f.value), f.tx, f.created)

        print()
        print(table)

