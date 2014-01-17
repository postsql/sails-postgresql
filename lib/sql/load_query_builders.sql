/*
helpers for testing in interactive python sesion

class PlPyEmulator:
    def quote_ident(this,s):
        return '"%s"' % s.replace('"', '""')
    def quote_literal(this,s):
        return "'%s'" % s.replace("'", "''")

plpy = PlPyEmulator()

*/

CREATE FUNCTION sails_postsql.load_query_builders() RETURNS void
    LANGUAGE plpythonu
    AS $$

opmap = {
    'like': 'ILIKE',
    'lessThan': '<',
    'lessThanOrEqual': '<=',
    'greaterThan': '>',
    'greaterThanOrEqual': '>=',
    'not': '<>',
    '!': '<>',
}

GD['opmap'] = opmap

def build_where_clause(options):
    if not options.has_key('where'):
        return []
    where = options['where']
    if not where: # null or empty 'where'
        return []
    query = ['WHERE']
    query.append(where_to_clause_list(where))
    return query

def where_to_clause_list(where):    
    where_list = []
    for key in where:
        value = where[key]
#        print (key, value, type(value))
        if key in ('or', 'and'): 
            or_list = [where_to_clause_list(or_item) for or_item in value]
#            print 'or_list:', or_list; print '(%s)' % ' OR '.join(or_list)
            where_list.append('(%s)' % (' %s ' % key.upper()).join(or_list))
            continue
        if key in ('not', '!'):
            if (type(value) == dict):
                where_list.append('(NOT %s)' % where_to_clause_list(value))
            elif (type(value) == list):
                not_list = [where_to_clause_list(or_item) for or_item in value]
                where_list.append('(NOT (%s))' % ' AND '.join(not_list))
            elif (type(value) in (str, unicode)):
                where_list.append('(NOT %s)' % plpy.quote_ident(value))
            continue
        if key in ('like','ilike'):
            for (likefield, likevalue) in value.items():
                where_list.append("%s %s %s" % (plpy.quote_ident(likefield),
                                                key.upper(),
                                                plpy.quote_literal(likevalue)))
            continue
        if value is None:
            where_list.append("%s IS NULL" % plpy.quote_ident(key))
        elif (type(value) in (str, unicode)):
            where_list.append("%s = %s" % (plpy.quote_ident(key),plpy.quote_literal(value)))
        elif (type(value) == dict):
            for opkey in value:
                opvalue = value[opkey]
                if opkey == 'startsWith':
#                     where_list.append("%s ~* %s" % (plpy.quote_ident(key),plpy.quote_literal('^'+opvalue)))
                    where_list.append("%s ILIKE %s" % (plpy.quote_ident(key),plpy.quote_literal(opvalue + '%')))
                    continue
                if opkey == 'contains':
                    where_list.append("%s ILIKE %s" % (plpy.quote_ident(key),plpy.quote_literal('%' + opvalue + '%')))
                    continue
                if opkey == 'endsWith':
#                    where_list.append("%s ~* %s" % (plpy.quote_ident(key),plpy.quote_literal(opvalue+'$')))
                    where_list.append("%s ILIKE %s" % (plpy.quote_ident(key),plpy.quote_literal('%' + opvalue)))
                    continue
                op = opmap.get(opkey, opkey)
                
                if (op,opvalue) == ('<>', None):
                    where_list.append("%s IS NOT NULL" % plpy.quote_ident(key))
                elif type(opvalue) == list:
                    # maybe should plpy.quote_literal(str()) if any are strings to match JS behaviour
                    in_list = [(plpy.quote_literal(item) if (type(item) in (str, unicode)) else item) for item in value]
                    where_list.append("%s %s ANY (%s)" % (plpy.quote_ident(key),  op, ','.join(in_list)))
                else:
                    if type(opvalue) in (str, unicode):
                        opvalue = plpy.quote_literal(opvalue)
                    where_list.append("%s %s %s" % (plpy.quote_ident(key), op, opvalue))
        elif (type(value) == list):
            # maybe should plpy.quote_literal(str()) if any are strings to match JS behaviour
            in_list = [(plpy.quote_literal(item) if (type(item) in (str, unicode)) else item) for item in value]
            where_list.append("%s IN (%s)" % (plpy.quote_ident(key), ','.join(in_list)))
        else:
            where_list.append("%s = %s" % (plpy.quote_ident(key), value))
    if len(where_list) == 1:
        return where_list[0]
    return '(%s)' % ' AND '.join(where_list)

"""
>>> print build_where_clause(json.loads('{"where":{"not":{"first_name":"OR_user10"}}}'))
['WHERE', u'(NOT "first_name" = \'OR_user10\')']
>>> build_where_clause(json.loads('{"where":{"not":{"first_name":"OR_user10"}}}'))
['WHERE', u'(NOT "first_name" = \'OR_user10\')']
>>> build_where_clause(json.loads('{"where":{"first_name":"OR_user10", "a":1, "b":null}}'))
['WHERE', u'("a" = 1 AND "first_name" = \'OR_user10\' AND "b" IS NULL)']
>>> build_where_clause(json.loads('{"where":{"not":[{"first_name":"OR_user10"},{"first_name":"OR_user11", "kana": null}]}}'))
['WHERE', u'(NOT ("first_name" = \'OR_user10\' AND ("first_name" = \'OR_user11\' AND "kana" IS NULL)))']
>>> build_where_clause(json.loads('{"where":{"first_name":"not ! test","age":{"not":null}}}'))
['WHERE', u'("first_name" = \'not ! test\' AND "age" IS NOT NULL)']
>>> build_where_clause(json.loads('{"where":{"first_name":"not ! test","not":{"age":null}}}'))
['WHERE', u'((NOT "age" IS NULL) AND "first_name" = \'not ! test\')']
>>> build_where_clause(json.loads('{"where":{"first_name":"not ! test","age":{"not":40}}}'))
['WHERE', u'("first_name" = \'not ! test\' AND "age" <> 40)']
>>> build_where_clause(json.loads('{"where":{"first_name":"not ! test","not":{"age":40}}}'))
['WHERE', u'((NOT "age" = 40) AND "first_name" = \'not ! test\')']
>>> build_where_clause(json.loads('{"where":{"first_name":"not ! test","not":"status"}}'))
['WHERE', u'((NOT "status") AND "first_name" = \'not ! test\')']
"""

def build_offset_and_limit(options):
    skip_list = []
    if options.has_key('skip'):
        skip_list += ['OFFSET %s' % options['skip']]
    if options.has_key('limit'):
        skip_list += ['LIMIT %s' % options['limit']]
    return skip_list

sql_agg = {
    'count':'COUNT',
    'sum':'SUM',
    'average':'AVG',
    'min':'MIN',
    'max':'MAX'
}

def build_group_by(options):
    select_list = []
    for aggfunc in ('count','sum','average','min','max'):
        if options.has_key(aggfunc):
            for fieldname in options[aggfunc]:
                select_list += ['%s(%s)::float AS %s' % (sql_agg[aggfunc], fieldname, fieldname)]
    group_by_list = []
    if options.has_key('groupBy'):
        group_by_list += ['GROUP BY']
        group_by_list += [', '.join([plpy.quote_ident(group_by_col) for group_by_col in options['groupBy']])]
        select_list += [plpy.quote_ident(group_by_col) for group_by_col in options['groupBy']]
    return select_list, group_by_list

def build_sort(options):
    if not options.has_key('sort'):
        return []
    sort = options['sort']
    if not sort:
        return []
    sort_list = ['ORDER BY']
    if (type(sort) == dict):
        # TODO, NB! - form multi-key sort this implements current broken behaviour, needs a switch to lists to preserve key order
        for (sortcol, sortdir) in sort.items():
            if len(sort_list) > 1:
                sort_list.append(',')
            sort_list += [plpy.quote_ident(sortcol), 'ASC' if (sortdir == 1) else 'DESC']
    elif (type(sort) == list):
        for sort_field in sort:
            # TODO: check that each spec is exactly one field
            if len(sort_list) > 1:
                sort_list.append(',')
            (sortcol, sortdir) = sort_field.items()[0]
            sort_list += [plpy.quote_ident(sortcol), 'ASC' if (sortdir == 1) else 'DESC']
    return sort_list

import json

def build_destroy_query(table, options_in):
    options = json.loads(options_in)
    query = ['DELETE FROM %s' % plpy.quote_ident(table)];
    query  += build_where_clause(options)
    # TODO: return actual PK column(s)
    query.append("RETURNING id")
    return ' '.join(query)

GD['build_destroy_query'] = build_destroy_query

def build_update_query(table, options_in, data_in):
    options = json.loads(options_in)
    data = json.loads(data_in).items()
    fields = [plpy.quote_ident(item[0]) for item in data]
    values = [plpy.quote_literal(str(item[1])) for item in data]
    query = ['UPDATE %s SET(%s) = (%s)' % (plpy.quote_ident(table),','.join(fields),','.join(values))]
#    print query, build_where_clause(options)
    query  += build_where_clause(options)
    query.append('RETURNING row_to_json(%s.*) AS res' % plpy.quote_ident(table))
    return ' '.join(query)

GD['build_update_query'] = build_update_query

def build_find_query(table, options_in):
    options = json.loads(options_in)
    query = ['SELECT',
             'row_to_json(%s.*) AS res' % plpy.quote_ident(table),
             'FROM %s' % plpy.quote_ident(table)];
    query += build_where_clause(options)
    query += build_offset_and_limit(options)
    select_list, group_by_list = build_group_by(options)
    query += group_by_list
    query += build_sort(options)
    if select_list:
        query[1] = ', '.join(select_list) # replace select list into main select
        return 'SELECT row_to_json(s.*) AS res FROM (%s)s' % ' '.join(query) # wrap in outer JSON-ify 
    else:
        return ' '.join(query)

GD['build_find_query'] = build_find_query


GD['query_builders_loaded'] = True

$$;

/*
LOG:  duration: 0.079 ms  bind <unnamed>: SELECT * FROM user_find($1);
DETAIL:  parameters: $1 = '{"where":{"type":"min test"},"min":["age"]}'
WARNING:  ('find_query', u'SELECT row_to_json(s.*) AS res FROM (SELECT min(age)::float AS age FROM "user" WHERE type = \'min test\')s')
CONTEXT:  PL/Python function "user_find"
WARNING:  ('>> %', '{"age":-9}')
CONTEXT:  PL/Python function "user_find"
LOG:  duration: 0.792 ms  execute <unnamed>: SELECT * FROM user_find($1);
DETAIL:  parameters: $1 = '{"where":{"type":"min test"},"min":["age"]}'
LOG:  duration: 0.167 ms  parse <unnamed>: SELECT * FROM user_find($1);
LOG:  duration: 0.064 ms  bind <unnamed>: SELECT * FROM user_find($1);
DETAIL:  parameters: $1 = '{"where":{"type":"min test"},"min":["age","percent"]}'
WARNING:  ('find_query', u'SELECT row_to_json(s.*) AS res FROM (SELECT min(age)::float AS age, min(percent)::float AS percent FROM "user" WHERE type = \'min test\')s')
CONTEXT:  PL/Python function "user_find"
WARNING:  ('>> %', '{"age":-9,"percent":-4.5}')
*/

/*
STATEMENT:  /*find*/ SELECT "\"user\"", {"where":{"type":"min test"},"min":["age"]}
LOG:  duration: 0.389 ms  parse <unnamed>: SELECT MIN(age) AS age FROM "user" WHERE LOWER("type") = $1
LOG:  duration: 0.544 ms  bind <unnamed>: SELECT MIN(age) AS age FROM "user" WHERE LOWER("type") = $1
DETAIL:  parameters: $1 = 'min test'
LOG:  duration: 0.578 ms  execute <unnamed>: SELECT MIN(age) AS age FROM "user" WHERE LOWER("type") = $1
DETAIL:  parameters: $1 = 'min test'
ERROR:  syntax error at or near "user" at character 20
STATEMENT:  /*find*/ SELECT "\"user\"", {"where":{"type":"min test"},"min":["age","percent"]}
LOG:  duration: 0.237 ms  parse <unnamed>: SELECT MIN(age) AS age, MIN(percent) AS percent FROM "user" WHERE LOWER("type") = $1
LOG:  duration: 0.559 ms  bind <unnamed>: SELECT MIN(age) AS age, MIN(percent) AS percent FROM "user" WHERE LOWER("type") = $1
DETAIL:  parameters: $1 = 'min test'
LOG:  duration: 0.377 ms  execute <unnamed>: SELECT MIN(age) AS age, MIN(percent) AS percent FROM "user" WHERE LOWER("type") = $1
DETAIL:  parameters: $1 = 'min test'
ERROR:  syntax error at or near "user" at character 20
*/
