from dataclasses import dataclass

from lark import Lark, Transformer


jql_parser = Lark(r"""
    action: "CREATE" content data*     -> create
          | "CREATE" data+             -> create
          | "SET" id content           -> set
          | "SET" id data+             -> set
          | "GET" id                   -> get
          | "HISTORY" id               -> history
          | "LIST" content data*       -> list
          | "LIST" data+               -> list

    ?data: tag
         | fact
         | value

    id      : "@" INT
    tag     : "#" WORD 
    fact    : tag "/" CNAME
    value   : fact "=" /[^ ]+/
    content : /[^#]+/

    %import common.WORD
    %import common.CNAME
    %import common.INT
    %import common.WS
    %ignore WS
    """, start='action')


@dataclass(frozen=True)
class ItemRef:
    id: str

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self)})"

    def __str__(self):
        return f"@{self.id}"


@dataclass(frozen=True)
class Tag:
    tag: str

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self)})"

    def __str__(self):
        return f"#{self.tag}"


@dataclass(frozen=True)
class Fact(Tag):
    fact: str

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self)})"

    def __str__(self):
        return f'{super().__str__()}/{self.fact}'


@dataclass(frozen=True)
class FactValue(Fact):
    value: str

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self)})"

    def __str__(self):
        return f'{super().__str__()}={self.value}'


@dataclass(frozen=True)
class Content(FactValue):
    def __init__(self, content: str):
        object.__setattr__(self, "tag", "db")
        object.__setattr__(self, "fact", "content")
        object.__setattr__(self, "value", content)

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self)})"

    def __str__(self):
        return self.value


class JqlTransformer(Transformer):
    def id(self, i):
        return ItemRef(i[0].value)

    def tag(self, i):
        return Tag(i[0].value)

    def fact(self, i):
        return Fact(i[0].tag, i[1].value)

    def value(self, i):
        return FactValue(i[0].tag, i[0].fact, i[1].value)

    def content(self, i):
        return Content(i[0].value.strip())

