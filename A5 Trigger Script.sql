-- Calculation Procedure 
--------------------------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS table_update;
CREATE OR REPLACE FUNCTION table_update(in_table_name varchar(30))
RETURNS void AS $$
DECLARE 
metadata_rec record;
colname varchar(30);
max_value NUMERIC;
min_value NUMERIC;
avg_value NUMERIC(10, 2);
max_length NUMERIC;
min_length NUMERIC;
nulls_count NUMERIC;
BEGIN
FOR metadata_rec IN 
SELECT column_name::varchar, data_type::varchar
FROM information_schema.columns
WHERE table_name = in_table_name
LOOP
IF metadata_rec.data_type IN ('integer', 'bigint', 'smallint', 'decimal', 'numeric', 'real', 'double precision') THEN
EXECUTE 'SELECT Max(' || quote_ident(metadata_rec.column_name)|| '),Min(' || quote_ident(metadata_rec.column_name)|| '),
round(AVG(' || quote_ident(metadata_rec.column_name)|| '),2),
COUNT(*) FILTER (WHERE ' || quote_ident(metadata_rec.column_name)|| ' IS NULL) AS null_count FROM ' || quote_ident(in_table_name) 
INTO max_value, min_value, avg_value, nulls_count;

RAISE NOTICE 'Table Name is %, Column Name is %, Maximum value is %, Minimum value is %, Avg %, Nulls counts is %', in_table_name,metadata_rec.column_name ,max_value, min_value, avg_value, nulls_count;

ELSEIF metadata_rec.data_type IN ('character varying', 'character', 'text') THEN
EXECUTE 'SELECT Max(Length(' || quote_ident(metadata_rec.column_name)|| ')),Min(Length(' || quote_ident(metadata_rec.column_name)|| ')),
COUNT(*) FILTER (WHERE ' || quote_ident(metadata_rec.column_name)|| ' IS NULL) AS null_count FROM ' || quote_ident(in_table_name)
INTO max_length, min_length, nulls_count;

RAISE NOTICE 'Table Name is %, Column_name is %, Max length %, Min length %, Nulls Count is %', in_table_name, metadata_rec.column_name ,max_length, min_length, nulls_count;
ELSE
EXECUTE 'SELECT COUNT(*) FILTER (WHERE ' || quote_ident(metadata_rec.column_name)|| ' IS NULL) AS null_count FROM ' || quote_ident(in_table_name) INTO nulls_count;

RAISE NOTICE 'Table Name is %, Column Name is %, Nulls Count is %', in_table_name, metadata_rec.column_name,nulls_count;

END IF;
END LOOP;
END;
$$ LANGUAGE PLPGSQL;

--------------------------------------------------------------------------------------------------------------------------------------------
--  Trigger Procedure
--------------------------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS tables_trigger;
CREATE OR REPLACE FUNCTION tables_trigger()
RETURNS TRIGGER AS $$
DECLARE 
table_name varchar(30);
BEGIN

table_name := TG_TABLE_NAME;
PERFORM table_update(table_name);

RETURN NEW;

END;
$$ LANGUAGE PLPGSQL;
----------------------------------------------------------------------------------------------------------------------------------------------
--- Procedure for Create Trigger on all the tables
----------------------------------------------------------------------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS create_trigger;
CREATE OR REPLACE PROCEDURE create_trigger()
AS $$
DECLARE 
table_rec record;
BEGIN
FOR table_rec IN
SELECT table_name
FROM information_schema.columns
WHERE table_schema = 'public'
LOOP
EXECUTE 'CREATE OR REPLACE TRIGGER trg_' || quote_ident(table_rec.table_name)|| ' AFTER UPDATE OR INSERT OR DELETE ON ' || quote_ident(table_rec.table_name)|| ' FOR EACH STATEMENT EXECUTE FUNCTION tables_trigger();';

END LOOP;
END;
$$ LANGUAGE PLPGSQL;

CALL create_trigger();
------------------------------------------------------------------------------------------------------------------------------------------------


