import sentry_sdk
from flask import abort, Flask, request, render_template, g, redirect, session, send_from_directory
from sentry_sdk.integrations.flask import FlaskIntegration
from itertools import filterfalse
import os
from typing import Any, Dict


if os.getenv("SENTRY_DSN"):
    sentry_sdk.init(
        integrations=[FlaskIntegration()],
        traces_sample_rate=0
    )


import webjql.lib as lib
from jql.types import is_tag, is_primary_ref, get_props, get_tags, get_flags, get_value, has_ref, has_sys_tag, Ref, single, Tag, Flag, Fact, tag_eq


app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET')


app.TAG_COLORS = {}
def get_tag_color(tag: str) -> str:
    global app
    if tag not in app.TAG_COLORS.keys():
        app.TAG_COLORS[tag] = len(app.TAG_COLORS) + 1
    return app.TAG_COLORS[tag]


@app.context_processor
def jql_utilities():  # type: ignore
    return dict(get_tags=get_tags, get_flags=get_flags, get_props=get_props, has_ref=has_ref, is_primary_ref=is_primary_ref, is_tag=is_tag)


@app.context_processor
def html_utilities() -> Dict[str, Any]:
    def make_link(fact: Fact, classes = None) -> str:
        if is_primary_ref(fact):
            link = f'{fact.value}'
        else:
            strfact = str(fact)
            url = lib.query_to_url(strfact)
            link = f'q/{url}'

        if classes:
            classes = ' '.join(classes)
        else:
            classes = ''

        return f'<a class="{classes}" href="/{g.database}/{link}">{fact}</a>'

    def make_button(fact: Fact) -> str:
        if is_tag(fact):
            color = get_tag_color(fact.tag)
            link = lib.query_to_url(str(fact))
            tag = fact.tag.lstrip('#')
            return f'<a class="button tagbutton tagbutton{color}" href="/{g.database}/{link}">{tag}</a>'
        else:
            return make_link(fact=fact)

    return dict(make_link=make_link, make_button=make_button)


@app.context_processor
def app_utilities():  # type: ignore
    return lib.get_toc()


@app.teardown_appcontext
def close_connection(exception):  # type: ignore
    client = getattr(g, '_client', None)
    if client is not None:
        del client


@app.route("/manifest.json")
def manifest():  # type: ignore
    return send_from_directory('static', 'manifest.json')


@app.route("/service-worker.js")
def serviceworker():  # type: ignore
    return send_from_directory('static', 'service-worker.js')


@app.route("/")
def list_dbs():  # type: ignore
    return render_template('dbs.html', databases=lib.update_databases())


@app.route("/<db>/NOW")
def create_db(db):  # type: ignore
    lib.create_database(db)
    return redirect(f'/{db}/')


@app.route("/<db>/query", methods=['POST'])
def post(db):  # type: ignore
    g.database = db
    query = request.form.get('q', '')
    referrer = request.form.get('referrer', '')

    if not len(query):
        return redirect(f'/{db}/')

    if query.startswith('@') and ' ' not in query:
        return redirect(f'/{db}/{query.lstrip("@")}')

    # Store referrer in case we need to redirect back
    if referrer:
        session[query] = referrer

    return redirect(f'/{db}/q/{lib.query_to_url(query)}')


@app.route("/<db>/", defaults={"query": ""})
@app.route("/<db>/q/<path:query>")
def query(db, query):  # type: ignore
    g.database = db

    client = lib.get_client()
    query = lib.url_to_query(query)

    # Try figure out the context
    context = []
    props = []

    tag = None
    if not len(query):
        tag = lib.get_toc()['primary_tag'].tag
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

    if tag and tag != "db":
        props = [(single(filterfalse(has_sys_tag, get_flags(t))), get_value(t, "db", "count")) for t in client.read(f"HINTS #{tag}/") if get_flags(t)]

    # Run query
    tx = client.new_transaction()
    items = tx.q(query or f'#{tag}')

    # Grab referrer out of session whether we use it or not (to stop the session dict growing and growing)
    referrer = session.pop(query, '')

    # If this is a write, redirect back to referrer
    if tx.changeset:
        if not referrer:
            return redirect(f'/{g.database}/')
        else:
            return redirect(f'/{g.database}/q/{lib.query_to_url(referrer)}')

    return render_template('tag.html', title=query, context=context, props=props, items=items)


@app.route("/<db>/<ref>")
def ref(db, ref):  # type: ignore
    g.database = db

    item = lib.get_client().read(f"@{ref}")[0]
    item_tags = [Tag('db')] + [t for t in get_tags(item) if next(filter(tag_eq(t.tag), get_props(item)), None) is not None]

    return render_template('ref.html', title=f'@{ref}', context=[Ref(ref)], item=item, item_tags=item_tags)


@app.route("/share", methods=['POST'])
def share():  # type: ignore
    db = 'jql'
    link = request.form.get('received_text', '')
    title = request.form.get('received_title', '')

    query = f'CREATE {title} {link} #tosort'

    return redirect(f'/{db}/q/{lib.query_to_url(query)}')
