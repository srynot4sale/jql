from parser import Content, Tag, FactValue


class ItemImmutableException(Exception):
    pass


class Item:
    """
    An item is a group of facts at a point in time
    """
    id: str
    timestamp: str
    facts: tuple

    _freeze = False

    def __init__(self, id, facts, timestamp=None):
        self.id = id
        self.timestamp = timestamp
        self.facts = set(facts)
        self._freeze = True

    def __setattr__(self, attr, val):
        if self._freeze:
            raise ItemImmutableException()
        super().__setattr__(attr, val)

    def __delattr__(self, attr):
        if self._freeze:
            raise ItemImmutableException()
        super().__delattr__(attr)

    def __repr__(self):
        f = ', '.join([repr(f) for f in self.facts])
        return f"Item(id={self.id} timestamp={self.timestamp} facts=[{f}])"

    def __str__(self):
        facts = []
        for f in self.facts:
            if isinstance(f, Content):
                facts.insert(0, str(f))
            else:
                facts.append(str(f))
        else:
            return f"@{self.id} {' '.join(facts)}"

    def to_dict(self):
        i = {}
        for t in self.tags():
            i[t] = {}
        for f in self.facts:
            if type(f) == Tag:
                continue
            i[f.tag][f.fact] = f.value if isinstance(f, FactValue) else True
        return i

    def tags(self):
        return {f.tag for f in self.facts if isinstance(f, Tag)}
