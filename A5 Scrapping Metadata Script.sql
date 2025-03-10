--DROP SCHEMA datascrapping;
--CREATE SCHEMA datascrapping AUTHORIZATION postgres;
---------------------------------------------------------------------------------------
-- Creation Of Metadata Scrapping Tables
---------------------------------------------------------------------------------------
DROP TABLE IF EXISTS tables_columns_dtypes;
CREATE TABLE tables_columns_dtypes(
table_name varchar(30),
col_name varchar(30),
col_dtype varchar (50),
no_records NUMERIC
);

DROP TABLE IF EXISTS numeric_columns;
CREATE TABLE numeric_columns(
table_name varchar(30),
column_name varchar(30),
number_records NUMERIC,
column_dtype varchar(50),
max_value NUMERIC,
min_value NUMERIC,
avg_value NUMERIC(10,2),
count_null_values NUMERIC
);

DROP TABLE IF EXISTS textual_columns;
CREATE TABLE textual_columns(
table_name varchar(30),
column_name varchar(30),
number_records NUMERIC,
column_dtype varchar(50),
max_length NUMERIC,
min_length NUMERIC,
count_null_values NUMERIC
);

DROP TABLE IF EXISTS other_columns;
CREATE TABLE other_columns(
table_name varchar(30),
column_name varchar(30),
number_records NUMERIC,
column_dtype varchar(50),
count_null_values NUMERIC
);
--------------------------------------------------------------------------------------------------
-- Function reads the metadata of public schema and insert it into "tables_columns_dtypes" Table
--------------------------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS into_table_column;
CREATE OR REPLACE FUNCTION into_table_column()
RETURNS void AS $$
DECLARE 
tab_rec record;
recs_count Numeric;
BEGIN
	FOR tab_rec IN
		SELECT table_name::varchar,
		column_name::varchar,
		data_type::varchar
		FROM information_schema.columns
		WHERE table_schema IN ('public')
	LOOP 
		EXECUTE 'SELECT COUNT(*) FROM '||quote_ident(tab_rec.table_name) INTO recs_count;
		INSERT INTO tables_columns_dtypes(table_name,col_name,col_dtype,no_records)
 		values(tab_rec.table_name,tab_rec.column_name,tab_rec.data_type,recs_count);
	END LOOP;
END;
$$ LANGUAGE PLPGSQL;

SELECT into_table_column();
SELECT * FROM tables_columns_dtypes;
-----------------------------------------------------------------------------------------------------------------------------------------------
-- Procedure reads "tables_columns_dtypes" Table and distributes it's data according to the datatype into "Numeric","Textual","Others" Tables
-----------------------------------------------------------------------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS tables_info;
CREATE OR REPLACE PROCEDURE tables_info()
AS $$
DECLARE tb_rec record;
max_val_c NUMERIC;
min_val_c NUMERIC;
avg_value_c NUMERIC(10,2);
min_vlength NUMERIC;
max_vlength NUMERIC;
null_val_c NUMERIC;
table_recs NUMERIC;
BEGIN
FOR tb_rec IN 
SELECT table_name,col_name,col_dtype
FROM tables_columns_dtypes
LOOP
IF tb_rec.col_dtype IN ('integer', 'bigint', 'smallint', 'decimal', 'numeric', 'real', 'double precision') THEN
EXECUTE 'SELECT Max(' || quote_ident(tb_rec.col_name)|| '),Min(' || quote_ident(tb_rec.col_name)|| '),AVG(' || quote_ident(tb_rec.col_name)|| '), COUNT(*),COUNT(*) FILTER (WHERE ' || quote_ident(tb_rec.col_name)|| ' IS NULL) AS null_count
FROM ' || quote_ident(tb_rec.table_name) INTO max_val_c, min_val_c, avg_value_c, table_recs, null_val_c;

INSERT INTO numeric_columns(table_name,column_name,number_records,column_dtype,max_value,min_value,avg_value,count_null_values) values(tb_rec.table_name,tb_rec.col_name,table_recs,tb_rec.col_dtype, max_val_c, min_val_c,avg_value_c, null_val_c);

RAISE NOTICE 'column Name is % is in Table %, Max value is %, Min value is %, Null counts is  %',tb_rec.col_name,tb_rec.table_name,max_val_c,min_val_c,null_val_c;

ELSEIF  tb_rec.col_dtype IN ('character varying', 'character', 'text') THEN
EXECUTE 'SELECT Max(Length('||quote_ident(tb_rec.col_name)||')),Min(Length('||quote_ident(tb_rec.col_name)||')), COUNT(*),COUNT(*) FILTER (WHERE '||quote_ident(tb_rec.col_name)||' IS NULL) AS null_count FROM '||quote_ident(tb_rec.table_name) INTO max_vlength,min_vlength,table_recs,null_val_c;

INSERT INTO textual_columns(table_name,column_name,number_records,column_dtype,max_length,min_length,count_null_values) values(tb_rec.table_name, tb_rec.col_name, table_recs,tb_rec.col_dtype, max_vlength, min_vlength, null_val_c);

RAISE NOTICE 'Column Name is % is in Table %, Max Length is %, Min Length is %, Null counts is   %',tb_rec.col_name,tb_rec.table_name,max_vlength,min_vlength,null_val_c;

ELSE
EXECUTE 'SELECT COUNT(*),COUNT(*) FILTER (WHERE '||quote_ident(tb_rec.col_name)||' IS NULL) AS null_count FROM '||quote_ident(tb_rec.table_name) INTO table_recs,null_val_c;

RAISE NOTICE 'column Name is % is in Table %, Null counts is %',tb_rec.col_name,tb_rec.table_name,null_val_c ;

INSERT INTO other_columns(table_name,column_name,number_records,column_dtype,count_null_values)
values(tb_rec.table_name, tb_rec.col_name, table_recs,tb_rec.col_dtype, null_val_c);

END IF; 
END LOOP;
END;
$$ LANGUAGE PLPGSQL;

CALL tables_info();
------------------------------------------------------------------------------------------------------




