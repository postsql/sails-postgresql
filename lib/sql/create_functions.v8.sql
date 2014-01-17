
---------------------------------------------------------------------------------
--
-- insert one row into user table, return inserted row as json object
--
---------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION sails_postsql.user_create(data_in json, OUT data_out json) LANGUAGE plv8
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
CREATE OR REPLACE FUNCTION sails_postsql.pkfactory_create(data_in json, OUT data_out json) LANGUAGE plv8
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
CREATE OR REPLACE FUNCTION sails_postsql.document_create(data_in json, OUT data_out json) LANGUAGE plv8
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
