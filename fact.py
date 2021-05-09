from dataclasses import dataclass


@dataclass
class F:
    tag: str
    fact: str
    value: str

    def is_tag(self):
        return self.fact is None

    def is_fact(self):
        return not self.is_tag()

    def get_key(self):
        return self.tag if self.fact is None else f'#{self.tag}/{self.fact}'

    def has_value(self):
        return self.value is not None

    @property
    def db_key(self):
        return f':{self.tag}' if self.fact is None else f'.{self.tag}_{self.fact}'

    def is_content(self):
        return self.tag == "db" and self.fact == "content"

    def as_string(self, markup=True):
        if self.is_content():
            return self.value

        output = f'[green][bold]#[/bold]{self.tag}[/green]' if markup else f'#{self.tag}'
        if self.is_fact():
            output += f'/[orange1]{self.fact}[/orange1]' if markup else f'/{self.fact}'
            if self.has_value():
                output += f'=[yellow]{self.value}[/yellow]' if markup else f'={self.value}'

        return output


@dataclass
class Fact(F):
    id: str
    tag: str
    fact: str
    value: str
    tx: str
    created: str

