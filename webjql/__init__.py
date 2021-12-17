from flask import abort, Flask, request, render_template, g, redirect
from itertools import filterfalse
from typing import Any, Dict


import webjql.lib as lib
from jql.types import is_tag, is_primary_ref, get_props, get_tags, get_flags, get_value, has_ref, has_sys_tag, Ref, single, Tag, Flag, Fact, tag_eq


app = Flask(__name__)


@app.context_processor
def jql_utilities():  # type: ignore
    return dict(get_tags=get_tags, get_flags=get_flags, get_props=get_props, has_ref=has_ref, is_primary_ref=is_primary_ref, is_tag=is_tag)


@app.context_processor
def html_utilities() -> Dict[str, Any]:
    def make_link(fact: Fact) -> str:
        if is_primary_ref(fact):
            link = f'{fact.value}'
        else:
            strfact = str(fact)
            url = lib.query_to_url(strfact)
            link = f'q/{url}'

        return f'<a href="/{g.database}/{link}">{fact}</a>'

    return dict(make_link=make_link)


@app.context_processor
def app_utilities():  # type: ignore
    return lib.get_toc()


@app.teardown_appcontext
def close_connection(exception):  # type: ignore
    client = getattr(g, '_client', None)
    if client is not None:
        del client


@app.route("/favicon.ico")
def notfound():  # type: ignore
    abort(404)


@app.route("/")
def list_dbs():  # type: ignore
    return render_template('dbs.html', databases=lib.update_databases())


@app.route("/<db>/NOW")
def create_db(db):  # type: ignore
    lib.create_database(db)
    return redirect(f'/{db}/')


@app.route("/<db>/query")
def results(db):  # type: ignore
    g.database = db
    query = request.args.get('q', '')

    if not len(query):
        return redirect(f'/{db}/')

    if query.startswith('@') and ' ' not in query:
        return redirect(f'/{db}/{query.lstrip("@")}')

    return redirect(f'/{db}/q/{lib.query_to_url(query)}')


@app.route("/<db>/", defaults={"query": ""})
@app.route("/<db>/q/<path:query>")
def query(db, query):  # type: ignore
    g.database = db

    query = lib.url_to_query(query)

    # Try figure out the context
    context = []
    props = []

    tag = None
    if not len(query):
        tag = lib.get_toc()['primary_tag'].tag
        query = f'#{tag}'
    else:
        if query.startswith('#') and ' ' not in query:
            q = query.lstrip('#')
            if '/' in q:
                q = q.split('/')
                context = [Flag(q[0], q[1])]
                tag = q[0]
            else:
                context = [Tag(q)]
                tag = q

    if tag:
        tx = lib.get_client().new_transaction()
        props = [(single(filterfalse(has_sys_tag, get_flags(t))), get_value(t, "db", "count")) for t in tx.q(f"HINTS #{tag}/") if get_flags(t)]

    tx = lib.get_client().new_transaction()
    items = tx.q(query)

    return render_template('tag.html', title=query, context=context, props=props, items=items)


@app.route("/<db>/<ref>")
def ref(db, ref):  # type: ignore
    g.database = db

    tx = lib.get_client().new_transaction()
    item = tx.q(f"@{ref}")[0]
    item_tags = [Tag('db')] + [t for t in get_tags(item) if next(filter(tag_eq(t.tag), get_props(item)), None) is not None]

    return render_template('ref.html', title=f'@{ref}', context=[Ref(ref)], item=item, item_tags=item_tags)
