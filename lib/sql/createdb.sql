
-- schema for sails-generated functions
CREATE SCHEMA IF NOT EXISTS sails_postsql;

-- put public before it so users can write their own functions to replace/enchance generated ones
ALTER DATABASE sailspg SET search_path TO public,sails_postsql;

CREATE EXTENSION IF NOT EXISTS plpythonu;
CREATE EXTENSION IF NOT EXISTS plv8;

CREATE EXTENSION IF NOT EXISTS citext;

-- for running waterline tests
\i create_waterline_test_tables.sql

-- handling of "options"
\i load_query_builders.sql

-- create functions
\i create_functions.sql

-- find functions
\i find_functions.sql

-- update functions
\i update_functions.sql

-- destroy functions
\i destroy_functions.sql

