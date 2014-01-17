/*
CREATE OR REPLACE FUNCTION sails_postsql.user_find(options_in json, OUT data_out json)
RETURNS SETOF json
LANGUAGE plpythonu
AS $$

if not GD.has_key('query_builders_loaded'):
   plpy.execute('SELECT load_query_builders()')

build_find_query = GD['build_find_query']

query = build_find_query('user', options_in)

plpy.warning('find_query', query)

res = plpy.execute(query)

if res:
    for row in res:
        plpy.warning('>> %', row['res'])
        yield row['res']

$$;
*/


CREATE OR REPLACE FUNCTION sails_postsql._make_find_function(dbobj text) 
RETURNS VOID LANGUAGE plpythonu
AS $_maker_$

query = """
CREATE OR REPLACE FUNCTION sails_postsql.%(dbobj)s_find(options_in json, OUT data_out json)
RETURNS SETOF json
LANGUAGE plpythonu
AS $$

if not GD.has_key('query_builders_loaded'):
   plpy.execute('SELECT load_query_builders()')

build_find_query = GD['build_find_query']

query = build_find_query('%(dbobj)s', options_in)

plpy.warning('find_query', query)

res = plpy.execute(query)

if res:
    for row in res:
        yield row['res']

$$;
""" % {'dbobj':dbobj}

plpy.execute(query)

$_maker_$;

SELECT sails_postsql._make_find_function('user');
SELECT sails_postsql._make_find_function('pkfactory');
SELECT sails_postsql._make_find_function('document');

