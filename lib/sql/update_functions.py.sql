/*
CREATE OR REPLACE FUNCTION sails_postsql.user_update(options_in json, data_in json, OUT data_out json)
RETURNS SETOF json
LANGUAGE plpythonu
AS $$

import json

if not GD.has_key('query_builders_loaded'):
   plpy.execute('SELECT load_query_builders()')

build_update_query = GD['build_update_query']

query = build_update_query('user', options_in, data_in)

open('/tmp/user_destroyquery.new','w').write(query + '\n')

res = plpy.execute(query)

if res:
    for row in res:
        plpy.warning('>> %', row['res'])
        yield row['res']

$$;


CREATE OR REPLACE FUNCTION sails_postsql.pkfactory_update(options_in json, data_in json, OUT data_out json)
RETURNS SETOF json
LANGUAGE plpythonu
AS $$

import json

if not GD.has_key('query_builders_loaded'):
   plpy.execute('SELECT load_query_builders()')

build_update_query = GD['build_update_query']

query = build_update_query('pkfactory', options_in, data_in)

open('/tmp/pkfactory_destroyquery.new','w').write(query + '\n')

res = plpy.execute(query)

if res:
    for row in res:
#        plpy.warning('>> %', row['res'])
        yield row['res']

$$;
*/



CREATE OR REPLACE FUNCTION sails_postsql._make_update_function(dbobj text) 
RETURNS VOID LANGUAGE plpythonu
AS $_maker_$

query = """
CREATE OR REPLACE FUNCTION sails_postsql.%(dbobj)s_update(options_in json, data_in json, OUT data_out json)
RETURNS SETOF json
LANGUAGE plpythonu
AS $$

import json

if not GD.has_key('query_builders_loaded'):
   plpy.execute('SELECT load_query_builders()')

build_update_query = GD['build_update_query']

query = build_update_query('%(dbobj)s', options_in, data_in)

res = plpy.execute(query)

if res:
    for row in res:
        yield row['res']

$$;
""" % {'dbobj':dbobj,'dbobj_q':plpy.quote_ident(dbobj)}

plpy.execute(query)

$_maker_$;

SELECT sails_postsql._make_update_function('user');
SELECT sails_postsql._make_update_function('pkfactory');
SELECT sails_postsql._make_update_function('document');
