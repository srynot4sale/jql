class FactImmutableException(Exception):
    pass


class F:
    tag: str
    fact: str
    value: str

    _freeze = False

    def __init__(self, tag: str, fact: str, value: str):
        self.tag = tag
        self.fact = fact
        self.value = value
        self._freeze = True

    def __setattr__(self, attr, val):
        if self._freeze:
            raise FactImmutableException()
        super().__setattr__(attr, val)

    def __delattr__(self, attr):
        if self._freeze:
            raise FactImmutableException()
        super().__delattr__(attr)

    def __repr__(self):
        return f"F({str(self)})"

    def __str__(self):
        if self.is_content():
            return self.value

        output = f'{self.key()}'
        if self.has_value():
            output += f'={self.value}'

        return output

    def is_tag(self):
        return self.fact is None

    def is_fact(self):
        return not self.is_tag()

    def key(self):
        return f'{self.tag}' if self.is_tag() else f'{self.tag}/{self.fact}'

    def has_value(self):
        return self.value is not None

    def is_content(self):
        return self.is_fact() and self.tag == "db" and self.fact == "content"


class Fact(F):
    _id: str
    tx: str
    created: str

    def __init__(self, tag: str, fact: str, value: str, _id: str, tx: str, created: str):
        self.tag = tag
        self.fact = fact
        self.value = value
        self._id = _id
        self.tx = tx
        self.created = created
        self._freeze = True
