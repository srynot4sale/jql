from typing import List, Tuple

from lark import Lark, Transformer, Token, Tree

from jql.types import Fact, Ref, Tag, Flag, Value, Content


jql_parser = Lark(r"""
    action: "CREATE" data+             -> create
          | "CREATE" content data*     -> create
          | "HINTS" prop*              -> hints
          | "CHANGESETS"               -> changesets
          | match "SET" content        -> set
          | match "SET" data+          -> set
          | id                         -> get
          | data+                      -> list
          | content data*              -> list
          | id "HISTORY"               -> history

    ?prop: tag "/"?
         | fact

    ?data: tag
         | fact
         | value

    ?match: id
          | data+
          | content data*

    id      : "@" ID
    tag     : "#" WORD
    fact    : tag "/" CNAME
    value   : fact "=" /[^ ]+/
    content : /[^#]+/

    ID      : HEXDIGIT+
    HEXDIGIT: "a".."f"|DIGIT
    %import common.WORD
    %import common.CNAME
    %import common.DIGIT
    %import common.WS
    %ignore WS
    """, start='action')


class JqlTransformer(Transformer[Tree]):
    def id(self, i: List[Token]) -> Fact:
        return Ref(i[0].value)

    def tag(self, i: List[Token]) -> Fact:
        return Tag(i[0].value)

    def fact(self, i: Tuple[Fact, Token]) -> Fact:
        return Flag(i[0].tag, i[1].value)

    def value(self, i: Tuple[Fact, Token]) -> Fact:
        return Value(i[0].tag, i[0].prop, i[1].value)

    def content(self, i: List[Token]) -> Fact:
        return Content(i[0].value.strip())
