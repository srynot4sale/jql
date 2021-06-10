from lark import Lark, Transformer

from jql.types import Ref, Tag, FactFlag, FactValue, Content


jql_parser = Lark(r"""
    action: "CREATE" data+             -> create
          | "CREATE" content data*     -> create
          | "SET" id content           -> set
          | "SET" id data+             -> set
          | "GET" id                   -> get
          | "HISTORY" id               -> history
          | "LIST" data+               -> list
          | "LIST" content data*       -> list

    ?data: tag
         | fact
         | value

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


class JqlTransformer(Transformer):
    def id(self, i) -> Ref:
        return Ref(i[0].value)

    def tag(self, i) -> Tag:
        return Tag(i[0].value)

    def fact(self, i) -> FactFlag:
        return FactFlag(i[0].tag, i[1].value)

    def value(self, i) -> FactValue:
        return FactValue(i[0].tag, i[0].fact, i[1].value)

    def content(self, i) -> Content:
        return Content(i[0].value.strip())
