/*
helpers for testing in interactive python sesion

class PlPyEmulator:
    def quote_ident(this,s):
        return '"%s"' % s.replace('"', '""')
    def quote_literal(this,s):
        return "'%s'" % s.replace("'", "''")

plpy = PlPyEmulator()

*/







CREATE OR REPLACE FUNCTION load_query_builders() RETURNS void
LANGUAGE plpythonu
AS $$

opmap = {
    'like': 'like',
    'lessThan': '<',
    'lessThanOrEqual': '<=',
    'greaterThan': '>',
    'greaterThanOrEqual': '>=',
    
}

GD['opmap'] = opmap

def build_where_clause(options):
    if not options.has_key('where'):
        return []
    query = ['WHERE']
    where = options['where']
    where_list = dict_to_clauses(where)
    query.append('(' + ' AND '.join(where_list) + ')')
    return query

"""

key:
   or
      or.join(process(value))
   and
      and.join(process(value))
   not
      if null
          'key is not null'
      elif not list or dict
          'key <> value'
      elif
        if isToplevelKey: # real top or in dict/list
          not + ( + and.join(process(value)) + )
        else

"""
    
def dict_to_clauses(where, join_with = 'AND')
    if not where: # null or empty 'where'
        return []
    where_list = []
    for key in where:
        if key == 'or':
            return '(%s)' % dict_to_clauses(value)
        elif key == 'and':
            # pull the sub-options up to main
            where_list += dict_to_clauses(value)
        elif key == 'not':
            if type(value) == dict:
                where_list += ['NOT', '(', ]
        else:
            value = where[key]
    #        print (key, value, type(value))
            if value is None:
                where_list.append("%s IS NULL" % plpy.quote_ident(key))
            elif (type(value) in (str, unicode)):
                where_list.append("%s = %s" % (plpy.quote_ident(key),plpy.quote_literal(value)))
            elif (type(value) == dict):
                for opkey in value:
                    op = opmap.get(opkey)
                    opvalue = value[opkey]
                    if type(opvalue) == list:
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
    return where_list


## these need to go to build_where_clause
#    if options.has_key('not')
#    if options.has_key('and')
#    if options.has_key('or')


##     
#    if options.has_key('skip'):
#    if options.has_key('limit'):
#    if options.has_key('sort'):    --
#       TODO" - "sort" value needs to be an Array, as object/dict does not preserve order
#    if options.has_key('distinct') -- not tested

sorts = """
hannu@hannu-900X3E:~/work/PostSQL/sails-postsql/test/integration$ grep options *.log | grep or
postsql-test.log:   options ={"where":{"type":"sort test"},"sort":{"dob":1}}
postsql-test.log:   options ={"where":{"type":"sort test"},"sort":{"dob":-1}}
postsql-test.log:   options ={"where":{"type":"sort test"},"sort":{"dob":1}}
postsql-test.log:   options ={"where":{"type":"sort test"},"sort":{"dob":-1}}
postsql-test.log:   options ={"where":{"type":"sort test"},"sort":{"dob":-1}}
postsql-test.log:   options ={"where":{"type":"sort test multi"},"sort":{"last_name":1,"first_name":-1}}
postsql-test.log:   options ={"where":{"type":"sort test multi"},"sort":{"last_name":1,"first_name":1}}
"""


ors = """
postsql-test.log:   options ={"where":{"or":[{"first_name":"OR_user0"},{"first_name":"OR_user1"}]}}
postsql-test.log:   options ={"where":{"or":[{"first_name":"OR_user0"},{"first_name":"OR_user1"}]}}
postsql-test.log:   options ={"where":{"or":[{"first_name":"OR_user10"},{"first_name":"OR_user11"}]}}
"""

nots = """
postsql-test.log:   options ={"where":{"first_name":"not ! test","age":{"not":40}}}
"""



##    if options.has_key('groupBy
#count
#sum
#min
#max
#average

import json

def build_update_query(table, options_in):
    options = json.loads(options_in)
    query = ['UPDATE %s SET' % plpy.quote_ident(table)];
    
    data = json.loads(data_in).items()
    ...
    
    query  += build_where_clause(options)
    query.append('RETURNING row_to_json("$s".*) AS res' % plpy.quote_ident(table) )
    return ' '.join(query)

GD['build_destroy_query'] = build_destroy_query

def build_destroy_query(table, options_in):
    options = json.loads(options_in)
    query = ['DELETE FROM %s' % plpy.quote_ident(table)];
    query  += build_where_clause(options)
    # TODO: return actual PK column(s)
    query.append("RETURNING id")
    return ' '.join(query)

GD['build_destroy_query'] = build_destroy_query

GD['query_builders_loaded'] = True

$$;

CREATE OR REPLACE FUNCTION user_create(data_in json, OUT data_out json)
LANGUAGE plpythonu
AS $$

import json

data = json.loads(data_in).items()

fields = [plpy.quote_ident(item[0]) for item in data]
values = [plpy.quote_literal(str(item[1])) for item in data]

query = 'INSERT INTO "user"(%s) VALUES (%s) RETURNING row_to_json("user".*) AS res' % (','.join(fields), ','.join(values))

res = plpy.execute(query)

#plpy.notice(query)

return res[0]['res']
$$;


