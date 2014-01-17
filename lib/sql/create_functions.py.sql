


---------------------------------------------------------------------------------
--
-- insert one row into user table, return inserted row as json object
--
---------------------------------------------------------------------------------
/*
CREATE OR REPLACE FUNCTION sails_postsql.user_create(data_in json, OUT data_out json)
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
*/


CREATE OR REPLACE FUNCTION sails_postsql._make_create_function(dbobj text) 
RETURNS VOID LANGUAGE plpythonu
AS $_maker_$

query = """
CREATE OR REPLACE FUNCTION sails_postsql.%(dbobj)s_create(data_in json, OUT data_out json)
LANGUAGE plpythonu
AS $$

import json

data = json.loads(data_in).items()

fields = [plpy.quote_ident(item[0]) for item in data]
values = [plpy.quote_literal(str(item[1])) for item in data]

query = 'INSERT INTO %(dbobj_q)s(%%s) VALUES (%%s) RETURNING row_to_json(%(dbobj_q)s.*) AS res' %% (','.join(fields), ','.join(values))

res = plpy.execute(query)

#plpy.notice(query)

return res[0]['res']
$$;""" % {'dbobj':dbobj,'dbobj_q':plpy.quote_ident(dbobj)}

plpy.execute(query)

$_maker_$;



CREATE OR REPLACE FUNCTION sails_postsql._make_create_function(dbobj text) 
RETURNS VOID LANGUAGE plpythonu
AS $_maker_$

query = """
CREATE OR REPLACE FUNCTION sails_postsql.%(dbobj)s_create(data_in json, OUT data_out json) LANGUAGE plv8
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

var query = 'INSERT INTO %(dbobj_q)s('+fields+') VALUES ('+placeholders+') RETURNING row_to_json(%(dbobj_q)s.*) AS res'

return plv8.execute(query, values)[0].res;

$$;
""" % {'dbobj':dbobj,'dbobj_q':plpy.quote_ident(dbobj)}

plpy.execute(query)

$_maker_$;





SELECT sails_postsql._make_create_function('user');
SELECT sails_postsql._make_create_function('pkfactory');
SELECT sails_postsql._make_create_function('document');


