from pathlib import Path

from lark import Lark, Transformer, Token, Tree, v_args  # type: ignore

from jql.types import Fact, Ref, Tag, Flag, Value, Content


grammar_file = Path(__file__).parent / 'jql.lark'
jql_parser = Lark(open(grammar_file), parser='lalr', start='action')


class JqlTransformer(Transformer[Tree]):  # type: ignore
    @v_args(inline=True)  # type: ignore
    def id(self, i: Token) -> Fact:
        return Ref(i.value)

    @v_args(inline=True)  # type: ignore
    def tag(self, i: Token) -> Fact:
        return Tag(i.value)

    @v_args(inline=True)  # type: ignore
    def fact(self, f: Fact, i: Token) -> Fact:
        return Flag(f.tag, i.value)

    @v_args(inline=True)  # type: ignore
    def value(self, f: Fact, i: Token) -> Fact:
        return Value(f.tag, f.prop, i.value)

    @v_args(inline=True)  # type: ignore
    def simpletext(self, i: Token) -> Fact:
        return Content(i.value.strip())

    @v_args(inline=True)  # type: ignore
    def quotedtext(self, i: Token) -> Fact:
        match = i.value
        if match.startswith('[[['):
            match = match[3:]
        if match.endswith(']]]'):
            match = match[:-3]
        return Content(match.strip())
