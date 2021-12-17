import yaml
from typing import Any, Callable, Dict, Tuple


from jql.types import Fact, Flag, Item, Tag


def make_item(result: Any) -> Item:
    facts = set()

    for t in result:
        if t != "db":
            facts.add(Tag(t))
        if not result[t]:
            continue
        for f in result[t]:
            v = result[t][f]
            if v is None:
                facts.add(Flag(t, f))
            else:
                facts.add(Fact(t, f, v))

    return Item(facts=facts)


def parse_refs(query: str, refs: Dict[str, Tuple[str, Item]]) -> str:
    for r in refs:
        query = query.replace(f'@{r}', refs[r][0])
    return query


def yamltest(func: Callable[[Any], str]) -> Callable[[Any], None]:
    def wrapper(db):  # type: ignore
        yml = func(db)
        testdef = yaml.safe_load(yml)

        print(testdef)

        refs: Dict[str, Tuple[str, Item]] = {}
        for step in testdef:
            query = step['q']
            ref = step.get('ref')
            ref_alias = step.get('ref_alias')

            with db.tx() as tx:
                tx.q(parse_refs(query, refs))

                if ref_alias:
                    if ref:
                        ref = make_item(ref)
                    refs[ref_alias] = (str(db.last_ref), ref)

                raw_results = step['result'] or []
                results = []
                for r in raw_results:
                    if isinstance(r, str):
                        results.append(refs[r][1])
                    else:
                        results.append(make_item(r))

                db.assert_result(results)

            if len(results) == 1:
                with db.tx() as tx:
                    tx.q(str(db.last_ref))
                    db.assert_result(results)

    return wrapper
