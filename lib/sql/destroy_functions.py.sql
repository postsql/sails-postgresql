/*
CREATE OR REPLACE FUNCTION sails_postsql.user_destroy(options_in json, OUT data_out json)
LANGUAGE plpythonu
AS $$

if not GD.has_key('query_builders_loaded'):
   plpy.execute('SELECT load_query_builders()')

build_destroy_query = GD['build_destroy_query']

query = build_destroy_query('user', options_in)
res = plpy.execute(query)

# TODO: return actual primary key column(s)
if res:
    return [row['id'] for row in res]

$$;
*/

CREATE OR REPLACE FUNCTION sails_postsql._make_destroy_function(dbobj text) 
RETURNS VOID LANGUAGE plpythonu
AS $_maker_$

query = """
CREATE OR REPLACE FUNCTION sails_postsql.%(dbobj)s_destroy(options_in json, OUT data_out json)
LANGUAGE plpythonu
AS $$

if not GD.has_key('query_builders_loaded'):
   plpy.execute('SELECT load_query_builders()')

build_destroy_query = GD['build_destroy_query']

query = build_destroy_query('%(dbobj)s', options_in)
res = plpy.execute(query)

# TODO: return actual primary key column(s)
if res:
    return [row['id'] for row in res]
$$;
""" % {'dbobj':dbobj,'dbobj_q':plpy.quote_ident(dbobj)}

plpy.execute(query)

$_maker_$;

SELECT sails_postsql._make_destroy_function('user');
SELECT sails_postsql._make_destroy_function('pkfactory');
SELECT sails_postsql._make_destroy_function('document');
