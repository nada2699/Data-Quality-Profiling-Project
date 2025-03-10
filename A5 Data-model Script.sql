--DROP SCHEMA metadata_data_model;
--CREATE SCHEMA metadata_data_model AUTHORIZATION postgres;
------------------------------------------------------------
-- Data Model Tables Creation 
-----------------------------------------------------------
DROP TABLE IF EXISTS table_metadata;
CREATE TABLE table_metadata(
table_id serial PRIMARY KEY,
table_name varchar(30),
table_records Numeric
);

DROP TABLE IF EXISTS column_metadata;
CREATE TABLE column_metadata(
column_id serial PRIMARY KEY,
column_name varchar(30),
column_dtype varchar(50),
table_id integer,
FOREIGN KEY (table_id) REFERENCES table_metadata (table_id)
);

DROP TABLE IF EXISTS numeric_cols;
CREATE TABLE numeric_cols(
numeric_colid serial PRIMARY KEY,
column_name varchar(30),
max_value NUMERIC,
min_value NUMERIC,
avg_value NUMERIC,
nulls_count NUMERIC,
refcolumn_id integer,
FOREIGN KEY (refcolumn_id) REFERENCES column_metadata (column_id)
);

DROP TABLE IF EXISTS textual_cols;
CREATE TABLE textual_cols(
text_colid serial PRIMARY KEY,
column_name varchar(30),
max_length NUMERIC,
min_length NUMERIC,
nulls_count NUMERIC,
refcolumn_id integer,
FOREIGN KEY (refcolumn_id) REFERENCES column_metadata (column_id)
);

DROP TABLE IF EXISTS otherdtype_cols;
CREATE TABLE otherdtype_cols(
odt_colid serial PRIMARY KEY,
column_name varchar(30),
nulls_count NUMERIC,
refcolumn_id integer,
FOREIGN KEY (refcolumn_id) REFERENCES column_metadata (column_id)
);

-----------------------------------------------------------------
-- Table's Insertions
-----------------------------------------------------------------
INSERT INTO table_metadata(table_name,table_records)
SELECT DISTINCT (tcd.table_name),no_records 
FROM datascrapping.tables_columns_dtypes tcd;

SELECT * FROM table_metadata tm;
-------------------------------------------------------------
INSERT INTO column_metadata(column_name,column_dtype,table_id)
SELECT tcd.col_name,tcd.col_dtype, tm.table_id 
FROM datascrapping.tables_columns_dtypes tcd
JOIN table_metadata tm
ON tm.table_name = tcd.table_name;

SELECT * FROM column_metadata cm ;

SELECT * 
FROM column_metadata cm 
JOIN table_metadata tm 
ON cm.table_id = tm.table_id ;
-----------------------------------------------------------------
INSERT INTO numeric_cols (column_name,max_value,min_value,avg_value,nulls_count,refcolumn_id)
SELECT ncs.column_name ,ncs.max_value ,ncs.min_value ,ncs.avg_value ,ncs.count_null_values,cm.column_id 
FROM datascrapping.numeric_columns ncs
JOIN column_metadata cm 
ON cm.column_name=ncs.column_name
JOIN table_metadata tm
ON cm.table_id=tm.table_id
AND tm.table_name =ncs.table_name;

SELECT * FROM numeric_cols nc ;

SELECT * 
FROM numeric_cols nc
join column_metadata cm 
on cm.column_id =nc.refcolumn_id
JOIN table_metadata tm 
on tm.table_id=cm.table_id;
------------------------------------------------------------------------------------------
INSERT INTO textual_cols (column_name,max_length,min_length,nulls_count,refcolumn_id)
SELECT tcs.column_name,tcs.max_length,tcs.min_length,tcs.count_null_values,cm.column_id 
FROM datascrapping.textual_columns tcs
JOIN column_metadata cm 
ON cm.column_name=tcs.column_name
JOIN table_metadata tm
ON cm.table_id=tm.table_id
AND tm.table_name =tcs.table_name;

SELECT * FROM textual_cols tc;

SELECT *
FROM textual_cols tcs
join column_metadata cm
on tcs.refcolumn_id =cm.column_id
JOIN table_metadata tm 
ON cm.table_id =tm.table_id;
-------------------------------------------------------------------------------------------------------
INSERT INTO otherdtype_cols (column_name,nulls_count,refcolumn_id)
SELECT os.column_name,os.count_null_values,cm.column_id 
FROM datascrapping.other_columns os
JOIN column_metadata cm 
ON cm.column_name=os.column_name
JOIN table_metadata tm
ON cm.table_id=tm.table_id
AND tm.table_name =os.table_name;

SELECT * FROM otherdtype_cols oc;

SELECT *
FROM otherdtype_cols oc
join column_metadata cm
on oc.refcolumn_id =cm.column_id
JOIN table_metadata tm 
ON cm.table_id =tm.table_id;
-----------------------------------------------------------------------------------------------------
--KPI's
-----------------------------------------------------------------------------------------------------
-- Total nulls across DB
SELECT (SELECT sum(nulls_count) as tex_nulls
FROM textual_cols tc) +
(SELECT sum(nulls_count) as numer_nulls
FROM numeric_cols nc)+
(SELECT sum(nulls_count) as other_nulls
FROM otherdtype_cols oc) AS Total_nulls;
-------------------------------------------------------------------------------------------------------
-- Top 10 Tables with null values
SELECT table_name,sum(total_nulls) as "total nulls"
FROM
(SELECT tm.table_name,sum(nc.nulls_count) as total_nulls
FROM numeric_cols nc 
join column_metadata cm
on nc.refcolumn_id =cm.column_id
JOIN table_metadata tm 
ON cm.table_id =tm.table_id
GROUP BY tm.table_name
union
SELECT tm.table_name,sum(tc.nulls_count) as total_nulls
FROM textual_cols tc 
join column_metadata cm
on tc.refcolumn_id =cm.column_id
JOIN table_metadata tm 
ON cm.table_id =tm.table_id
GROUP BY tm.table_name
union
SELECT tm.table_name,sum(oc.nulls_count) as total_nulls
FROM otherdtype_cols oc
join column_metadata cm
on oc.refcolumn_id =cm.column_id
JOIN table_metadata tm 
ON cm.table_id =tm.table_id
GROUP BY tm.table_name)
GROUP by table_name
ORDER BY "total nulls" DESC
LIMIT 10;
----------------------------------------------------------------------------------------------------
--Number of columns that have 100% nulls
SELECT * 
FROM table_metadata tm
JOIN column_metadata cm
ON tm.table_id =cm.table_id;

SELECT * FROM numeric_cols nc; 
SELECT * FROM textual_cols tc;
SELECT * FROM otherdtype_cols oc; 
SELECT * FROM table_metadata tm;

SELECT column_name,table_name,table_records,nulls_count
FROM
(SELECT nc.column_name,tm.table_name,tm.table_records,nc.nulls_count 
FROM numeric_cols nc
join column_metadata cm 
on nc.refcolumn_id = cm.column_id
join table_metadata tm 
on cm.table_id =tm.table_id
union
SELECT tc.column_name,tm.table_name,tm.table_records,tc.nulls_count
FROM textual_cols tc
join column_metadata cm 
on tc.refcolumn_id = cm.column_id
join table_metadata tm 
on tm.table_id =tm.table_id
union
SELECT oc.column_name,tm.table_name,tm.table_records,oc.nulls_count
FROM otherdtype_cols oc
join column_metadata cm 
on oc.refcolumn_id = cm.column_id
join table_metadata tm 
on tm.table_id =tm.table_id) 
WHERE nulls_count=table_records
AND table_records <> 0;


