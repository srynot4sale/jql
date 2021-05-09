def parse(query):
    tokens = query.split(' ')

    action = tokens[0].lower()

    raw = []
    curr = []
    values = []
    for token in tokens[1:]:
        if token.startswith('#'):
            if curr:
                raw.append(' '.join(curr))
                curr = []
            raw.append(token)
        elif token.startswith('@'):
            if curr:
                raw.append(' '.join(curr))
                curr = []
            raw.append(token)
        else:
            curr.append(token)
    if curr:
        raw.append(' '.join(curr))

    for r in raw:
        if r.startswith('@'):
            values.append(('id', r.lstrip('@')))
            continue

        if not r.startswith('#'):
            values.append(('content', r))
            continue

        if "/" in r:
            tag, fact = r.split("/", 1)
        else:
            tag = r
            fact = None

        tag = tag.lstrip('#')
        if fact is None:
            values.append(('fact', (tag, None, None)))
        else:
            if '=' in fact:
                f, v = fact.split('=', 1)
                values.append(('fact', (tag, f, v)))
            else:
                values.append(('fact', (tag, fact, None)))

    #print(f'action: {action}')
    #print(f'raw: {raw}')
    #print(f'values: {values}')
    return (query, action, raw, values)
