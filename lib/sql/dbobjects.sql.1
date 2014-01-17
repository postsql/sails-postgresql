
CREATE OR REPLACE FUNCTION user_create(data_in json, OUT data_out json)
LANGUAGE plpgsql
AS $$
BEGIN
    WITH ins AS (INSERT INTO "user"
    SELECT * from json_populate_record(NULL::"user", data_in)
    RETURNING *)
    SELECT row_to_json(ins.*) FROM ins
    INTO data_out;
END;
$$;

---------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION user_create(data_in json, OUT data_out json) LANGUAGE plv8
AS $$

var fields = [],
    placeholders = [],
    values = [];

var i = 1;
for (field in data_in) {
    fields.push(plv8.quote_ident(field));
    placeholders.push( '$' + i++ );
    values.push(data_in[field]);
};

var query = 'INSERT INTO "user"('+fields+') VALUES ('+placeholders+') RETURNING row_to_json("user".*) AS res'

return plv8.execute(query, values)[0].res;

$$;

---------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pkfactory_create(data_in json, OUT data_out json) LANGUAGE plv8
AS $$

var fields = [],
    placeholders = [],
    values = [];

var i = 1;
for (field in data_in) {
    fields.push(plv8.quote_ident(field));
    placeholders.push( '$' + i++ );
    values.push(data_in[field]);
};

var query = 'INSERT INTO "pkfactory"('+fields+') VALUES ('+placeholders+') RETURNING row_to_json("pkfactory".*) AS res'

return plv8.execute(query, values)[0].res;

$$;

---------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION document_create(data_in json, OUT data_out json) LANGUAGE plv8
AS $$

var fields = [],
    placeholders = [],
    values = [];

var i = 1;
for (field in data_in) {
    fields.push(plv8.quote_ident(field));
    placeholders.push( '$' + i++ );
    values.push(data_in[field]);
};

var query = 'INSERT INTO "document"('+fields+') VALUES ('+placeholders+') RETURNING row_to_json("document".*) AS res'

return plv8.execute(query, values)[0].res;

$$;

---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
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

---------------------------------------------------------------------------------

SELECT * FROM user_create('{"first_name":"FooBar","favoriteFruit":"blueberry","status":false,"createdAt":"2014-01-10T23:38:39.936Z","updatedAt":"2014-01-10T23:38:39.936Z"}');

CREATE OR REPLACE FUNCTION user_update(options_in json, data_in json, OUT data_out json)
LANGUAGE plpythonu
AS $$

if not GD.has_key('opmap'):
   plpy.execute('SELECT init_query_builders()')
opmap = GD['opmap'] 
user_update = GD['user_update']

query = user_update(options_in, data_in)
res = plpy.execute(query)
plpy.notice(query)

return res

$$;

---------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION user_destroy(options_in json, OUT data_out json)
LANGUAGE plpythonu
AS $$

if not GD.has_key('opmap'):
   plpy.execute('SELECT init_query_builders()')
#opmap = GD['opmap'] 
user_destroy = GD['user_destroy']

query = user_destroy(options_in)

open('/tmp/user_destroyquery.old','w+').write(query + '\n')

res = plpy.execute(query)
#plpy.notice(query)
if res:
    return [row['id'] for row in res]

$$;

---------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION init_query_builders() RETURNS void
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

import json

def user_destroy(options_in):
    options = json.loads(options_in)
    query = ['DELETE FROM "user"'];
    if options.has_key('where'):
        where = options['where']
        if where:
            query.append("WHERE")
            where_list = []
            for key in where:
                value = where[key]
                print (key, value, type(value))
                if value is None:
                    where_list.append("%s IS NULL" % plpy.quote_ident(key))
                elif (type(value) in (str, unicode)):
                    where_list.append("%s = %s" % (plpy.quote_ident(key),plpy.quote_literal(value)))
                elif (type(value) == dict):
                    for opkey in value:
                        op = opmap.get(opkey)
                        opvalue = value[opkey]
                        if type(opvalue) == str:
                            opvalue = plpy.quote_literal(opvalue)
                        where_list.append("%s %s %s" % (plpy.quote_ident(key), op, opvalue))
                elif (type(value) == list):
                    in_list = [(plpy.quote_literal(item) if (type(item) in (str, unicode)) else item) for item in value]
                    where_list.append("%s IN (%s)" % (plpy.quote_ident(key), ','.join(in_list)))
                else:
                    where_list.append("%s = %s" % (plpy.quote_ident(key), value))
            query.append(' AND '.join(where_list))
    query.append("RETURNING id")
    return ' '.join(query)

def user_update(options_in, data_in):
    pass

GD['user_destroy'] = user_destroy

$$;



---------------------------------------------------------------------------------
---------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION user_destroy(options_in json, OUT data_out json)
LANGUAGE plpythonu
AS $$

opmap = {
    'like': 'like',
    'lessThan': '<',
    'lessThanOrEqual': '<=',
    'greaterThan': '>',
    'greaterThanOrEqual': '>=',
    
}

class plpyc:
    def quote_ident(this,s):
        return '"%s"' % s.replace('"', '""')
    def quote_literal(this,s):
        return "'%s'" % s.replace("'", "''")

plpy = plpyc()

#def user_destroy(options_in):
    import json
    options = json.loads(options_in)
    query = ['DELETE FROM "user"'];
    if options.has_key('where'):
        where = options['where']
        if where:
            query.append("WHERE")
            where_list = []
            for key in where:
                value = where[key]
                print (key, value, type(value))
                if value is None:
                    where_list.append("%s IS NULL" % plpy.quote_ident(key))
                elif (type(value) in (str, unicode)):
                    where_list.append("%s = %s" % (plpy.quote_ident(key),plpy.quote_literal(value)))
                elif (type(value) == dict):
                    for opkey in value:
                        op = opmap.get(opkey)
                        opvalue = value[opkey]
                        if type(opvalue) == str:
                            opvalue = plpy.quote_literal(opvalue)
                        where_list.append("%s %s %s" % (plpy.quote_ident(key), op, opvalue))
                elif (type(value) == list):
                    in_list = [(plpy.quote_literal(item) if (type(item) in (str, unicode)) else item) for item in value]
                    where_list.append("%s IN (%s)" % (plpy.quote_ident(key), ','.join(in_list)))
                else:
                    where_list.append("%s = %s" % (plpy.quote_ident(key), value))
            query.append(' AND '.join(where_list))
    query.append("RETURNING id")
    return ' '.join(query)

#    if options.has_key('skip'):
#    if options.has_key('limit'):
#    if options.has_key('sort'):
#    if options.has_key('distinct'):
#    if options.has_key('groupBy
#    if options.has_key('not')
#    if options.has_key('and')
#    if options.has_key('or')






   criteria ={"where":{"first_name":"Destroy"}}
   criteria ={"where":{"id":15}}



query = 'INSERT INTO "user"(%s) VALUES (%s) RETURNING row_to_json("user".*) AS res' % (','.join(fields), ','.join(values))

res = plpy.execute(query)

#plpy.notice(query)

return res[0]['res']
$$;


   criteria ={"where":{"first_name":"Destroy"}}
   criteria ={"where":{"id":15}}

   options ={"where":{"first_name":"greaterThan test","age":{"greaterThan":40}}}
   options ={"where":{"first_name":"greaterThan test","age":{">":40}}}
   options ={"where":{"age":{"lessThanOrEqual":49}}}
   options ={"where":{"type":"find test"}}
   options ={"where":null}
   options ={"where":{"first_name":"findOne test"},"limit":1}
   options ={"where":{"first_name":"findOne test"},"limit":1}
   options ={"where":{"first_name":"findOne blah"},"limit":1}
   options ={"where":{"id":32},"limit":1}
   options ={"where":{"first_name":"findOrCreate()"}}
   options ={"where":{"first_name":"findOrCreate()"}}
   options ={"where":{"first_name":"findOrCreate()"}}
   options ={"where":{"first_name":"model findOrCreate()"}}
   options ={"where":{"type":"findOrCreateEach([])","first_name":"NOT IN THE SET"},"limit":1}
   options ={"where":{"type":"findOrCreateEach([])","first_name":"NOT IN THE SET"},"limit":1}
   options ={"where":{"first_name":"NOT IN THE SET"}}
   options ={"where":{"type":null,"first_name":"NOT IN THE SET"},"limit":1}
   
[0]:LOG:  duration: 0.303 ms  parse <unnamed>: DELETE FROM "user" WHERE LOWER("first_name") = $1 RETURNING id
[0]:LOG:  duration: 0.239 ms  bind <unnamed>: DELETE FROM "user" WHERE LOWER("first_name") = $1 RETURNING id
[263104]:LOG:  duration: 0.161 ms  execute <unnamed>: DELETE FROM "user" WHERE LOWER("first_name") = $1 RETURNING id
[0]:LOG:  duration: 0.258 ms  parse <unnamed>: DELETE FROM "user" WHERE "id" = $1 RETURNING id
[0]:LOG:  duration: 0.271 ms  bind <unnamed>: DELETE FROM "user" WHERE "id" = $1 RETURNING id
[263106]:LOG:  duration: 0.131 ms  execute <unnamed>: DELETE FROM "user" WHERE "id" = $1 RETURNING id
[0]:LOG:  duration: 12.263 ms  statement: DELETE FROM "user"  RETURNING id
[0]:LOG:  duration: 7.455 ms  statement: DELETE FROM "user"  RETURNING id
[0]:LOG:  duration: 11.539 ms  statement: DELETE FROM "user"  RETURNING id







CREATE TABLE "document" ("title" TEXT PRIMARY KEY UNIQUE,
    "number" SERIAL,
    "serialNumber" TEXT  UNIQUE,
    "createdAt" TIMESTAMP WITH TIME ZONE,
    "updatedAt" TIMESTAMP WITH TIME ZONE);

CREATE TABLE "pkfactory" ("number" INT,
    "createdAt" TIMESTAMP WITH TIME ZONE,
    "updatedAt" TIMESTAMP WITH TIME ZONE,
    "pkColumn" TEXT PRIMARY KEY UNIQUE);

CREATE TABLE "user" ("first_name" TEXT,
    "last_name" TEXT,
    "email" TEXT,
    "title" TEXT,
    "phone" TEXT,
    "type" TEXT,
    "favoriteFruit" TEXT,
    "age" INT,
    "dob" TIMESTAMP WITH TIME ZONE,
    "status" BOOLEAN,
    "percent" FLOAT,
    "list" JSON,
    "obj" JSON,
    "id" SERIAL PRIMARY KEY UNIQUE,
    "createdAt" TIMESTAMP WITH TIME ZONE,
    "updatedAt" TIMESTAMP WITH TIME ZONE
);
