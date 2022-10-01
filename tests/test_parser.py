import pytest
from typing import Any, List

from jql.parser import jql_parser, JqlTransformer
from jql.types import Content, Flag, Ref, Tag, Value


examples = [
    [
        "CREATE hi",
        ["create", [Content("hi")]]
    ],
    [
        "CREATE go to supermarket #todo #todo/completed",
        ["create", [Content("go to supermarket"), Tag("todo"), Flag("todo", "completed")]]
    ],
    [
        "CREATE do dishes #todo #chores",
        ["create", [Content("do dishes"), Tag("todo"), Tag("chores")]]
    ],
    [
        "CREATE book appointment #todo #todo/remind_at=20210412",
        ["create", [Content("book appointment"), Tag("todo"), Value("todo", "remind_at", "20210412")]]
    ],
    [
        "@d2a SET #todo/completed",
        ["set", [Ref("d2a"), Flag("todo", "completed")]]
    ],
    [
        "@3dd SET book appointment at physio",
        ["set", [Ref("3dd"), Content("book appointment at physio")]]
    ],
    [
        "@544 SET #book #todo",
        ["set", [Ref("544"), Tag("book"), Tag("todo")]]
    ],
    [
        "@a4f SET #f1 #trapped",
        ["set", [Ref("a4f"), Tag("f1"), Tag("trapped")]]
    ],
    [
        "@4af DEL #book",
        ["del", [Ref("4af"), Tag("book")]]
    ],
    [
        "@aaa",
        ["get", [Ref("aaa")]]
    ],
    [
        "@f4a HISTORY",
        ["history", [Ref("f4a")]]
    ],
    [
        "@f4a ARCHIVE",
        ["archive", [Ref("f4a")]]
    ],
    [
        "#todo/completed",
        ["list", [Flag("todo", "completed")]]
    ],
    [
        "do dishes",
        ["list", [Content("do dishes")]]
    ],
    [
        "find #todo",
        ["list", [Content("find"), Tag("todo")]]
    ],
    [
        "#todo/remind_at=444",
        ["list", [Value("todo", "remind_at", "444")]]
    ],
    [
        "HINTS",
        ["hints", []]
    ],
    [
        "HINTS #to",
        ["hints", [Tag("to")]]
    ],
    [
        "HINTS #to/",
        ["hints", [Tag("to")]]
    ],
    [
        "CREATE [[[ Delimited content capture ]]] #help",
        ["create", [Content("Delimited content capture"), Tag("help")]]
    ],
    [
        "CREATE [[[ This is not a #tag ]]] #help",
        ["create", [Content("This is not a #tag"), Tag("help")]]
    ],
    [
        "CREATE [[[ This is not the end ] and this is not a #tag ]]] #help",
        ["create", [Content("This is not the end ] and this is not a #tag"), Tag("help")]]
    ],
    [
        "CREATE [[[ This is not the end ]] and this is not a #tag ]]] #help",
        ["create", [Content("This is not the end ]] and this is not a #tag"), Tag("help")]]
    ],
    [
        "CREATE [[[ We can now have \n\n multiline!\n content OMG ]]] #help",
        ["create", [Content("We can now have \n\n multiline!\n content OMG"), Tag("help")]]
    ],
    [
        "CREATE [ OK I'm trying to trick you ]",
        ["create", [Content("[ OK I'm trying to trick you ]")]]
    ],
    [
        "CREATE [[ OK I'm trying to trick you ]]",
        ["create", [Content("[[ OK I'm trying to trick you ]]")]]
    ],
    [
        "CREATE OK I'm [[[ trying ]]] to trick you",
        ["create", [Content("OK I'm [[[ trying ]]] to trick you")]]
    ],
    [
        "CREATE book appointment #todo #todo/location=[[[ 31 Terrace Road,\nCitytown #CAL ]]]",
        ["create", [Content("book appointment"), Tag("todo"), Value("todo", "location", "31 Terrace Road,\nCitytown #CAL")]]
    ],
]


failure_examples = [
    # tags can't start with a number
    '@aaa SET #1f',
    # tags can't be uppercase
    '@aaa SET #FFF',
    # tags can't contain underscores
    '@aaa SET #f_f',
    # props can't be uppercase
    '@aaa SET #fine/NOTFINE',
    # props can't start with a number or underscore
    '@aaa SET #fine/1notfine',
    '@aaa SET #fine/_notfine',
    # can't specific a tag first after create
    'CREATE #help This is me',
    # can't start a quoted content n not finish
    'CREATE [[[ here is some content thats unfinished',
]


@pytest.mark.parametrize("test", examples)
def test_parser(test: List[Any]) -> None:
    query, (action, result) = test

    tree = jql_parser.parse(query)
    ast = JqlTransformer().transform(tree)

    assert ast.data == action
    assert ast.children == result


@pytest.mark.parametrize("test", failure_examples)
def test_parser_fails(test: str) -> None:

    with pytest.raises(Exception):
        res = jql_parser.parse(test)
        # We only show printed output if the parser doesn't throw
        # an exception like we are expecting
        print(res)
