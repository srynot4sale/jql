import re
import yaml
from typing import Any, Callable, Dict, Set, Tuple


from jql.types import Fact, Flag, Item, Tag, get_ref


def make_item(result: Any) -> Item:
    facts = set()

    for t in result:
        r = result[t]
        if t == "db":
            t = "_db"

        if t != "_db":
            facts.add(Tag(t))

        if not r:
            continue

        for f in r:
            v = r[f]
            if v is None:
                facts.add(Flag(t, f))
            else:
                facts.add(Fact(t, f, v))

    return Item(facts=facts)


def remove_variables_from_tuples(s: Set[Tuple[str, str, str]]) -> Set[Tuple[str, str, str]]:
    rm = [
        (r'@[a-f0-9]{6}', '??????'),  # @ref
        (r'Ref\(\'[a-f0-9]{6}\'\)', 'Ref(\'??????\')'),  # Ref(ref)
        (r'"ref"\: "[a-f0-9]{6}"', '"ref": "??????"'),  # "ref": "ref"
        (r'[0-9]{4}\-[0-9]{2}\-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}', '????-??-??'),  # Datetime
        (r'[a-f0-9]{8}\-[a-f0-9]{4}\-[a-f0-9]{4}\-[a-f0-9]{4}\-[a-f0-9]{12}', '????????'),  # uuid
    ]

    cleaned = set()
    for t in s:
        ct = []
        for i in t:
            for r in rm:
                i = re.sub(r[0], r[1], i)
            ct.append(i)

        cleaned.add(tuple(ct))

    return cleaned  # type: ignore


def yamltest(func: Callable[[Any], str]) -> Callable[[Any], None]:
    def wrapper(db):  # type: ignore
        yml = func(db)
        testdef = yaml.safe_load(yml)
        if not testdef:
            return

        refs: Dict[str, Tuple[str, Item]] = {}
        for step in testdef:
            query = step['q']

            with db.tx() as tx:
                tree = tx.query_to_tree(query, replacements=[(k, refs[k][0]) for k in refs])
                results = tx.q(query, tree)

                expected_results = step['result'] or []

                i = 0
                for r in expected_results:
                    ref = r.get('key', None)
                    if ref:
                        del r['key']

                    if ref:
                        if r:
                            if ref in refs:
                                raise Exception('duplicate ref')
                            refs[ref] = (get_ref(results[i]).value, make_item(r))

                        item = refs[ref][1]
                    else:
                        item = make_item(r)

                    result_tuple = results[i].as_tuples()
                    expected_tuple = item.as_tuples()

                    print(f'{str(result_tuple)} vs {str(expected_tuple)}')
                    result_tuple = remove_variables_from_tuples(result_tuple)
                    print(f'{str(result_tuple)} vs {str(expected_tuple)}')

                    assert result_tuple == expected_tuple

                    i += 1

    return wrapper
