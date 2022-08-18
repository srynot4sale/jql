from pathlib import Path
from typing import List, Tuple

from lark import Lark, Transformer, Token, Tree  # type: ignore

from jql.types import Fact, Ref, Tag, Flag, Value, Content


grammar_file = Path(__file__).parent / 'jql.lark'
jql_parser = Lark(open(grammar_file), start='action')


class JqlTransformer(Transformer[Tree]):  # type: ignore
    def id(self, i: List[Token]) -> Fact:
        return Ref(i[0].value)

    def tag(self, i: List[Token]) -> Fact:
        return Tag(i[0].value)

    def fact(self, i: Tuple[Fact, Token]) -> Fact:
        return Flag(i[0].tag, i[1].value)

    def value(self, i: Tuple[Fact, Token]) -> Fact:
        return Value(i[0].tag, i[0].prop, i[1].value)

    def simpletext(self, i: List[Token]) -> Fact:
        return Content(i[0].value.strip())

    def quotedtext(self, i: List[Token]) -> Fact:
        match = i[0].value
        if match.startswith('[[['):
            match = match[3:]
        if match.endswith(']]]'):
            match = match[:-3]
        return Content(match.strip())
