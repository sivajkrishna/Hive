# Hive
-----------------------------1Hive-DDL-----------------------------------------------------------------------

1) Database DDL

HIVE Databases DDL Script:

CREATE DATABASE hivepractice;
CREATE DATABASE IF NOT EXISTS hivepractice;

-- How to overwrite the default location of database, mentioned in warehouse directory?

CREATE DATABASE IF NOT EXISTS hivepractice
COMMENT 'hive practice database'
LOCATION '/hive/practice'
WITH DBPROPERTIES ('creator'='inventateq','date'='2016-06-01');

-- Display databases
SHOW DATABASES;	
SHOW DATABASES LIKE 'h.*';
DESCRIBE DATABASE default;
DESCRIBE DATABASE hivepractice;

--Use the database
USE hivepractice;

--Drop the empty database.
DROP DATABASE IF EXISTS hivepractice;

--Drop non empty database with CASCADE
DROP DATABASE IF EXISTS hivepractice CASCADE;

--metadata about database could not be changed.
ALTER DATABASE hivepractice SET DBPROPERTIES ('updated-by' = 'Dora');


2)Datatype DDL :

Hive Data Types 

--Create table using ARRAY, MAP, STRUCT, and Composite data type
CREATE TABLE employee
(
  name string,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

--Verify tables creations run in Beeline
!table employee

--Load data
LOAD DATA LOCAL INPATH '/home/osboxes/scripts/employee_data.txt' OVERWRITE INTO TABLE employee;

--Query the whole table
SELECT * FROM employee;

--Query the ARRAY in the table
SELECT work_place FROM employee;

SELECT work_place[0] AS col_1, work_place[1] AS col_2, work_place[2] AS col_3 FROM employee;

--Query the STRUCT in the table
SELECT gender_age FROM employee;

SELECT gender_age.gender, gender_age.age FROM employee;

--Query the MAP in the table
SELECT skills_score FROM employee;

SELECT name, skills_score['DB'] AS DB, 
skills_score['Perl'] AS Perl, skills_score['Python'] AS Python, 
skills_score['Sales'] as Sales, skills_score['HR'] as HR FROM employee;

SELECT depart_title FROM employee;

SELECT name, depart_title['Product'] AS Product, depart_title['Test'] AS Test,
depart_title['COE'] AS COE, depart_title['Sales'] AS Sales  
FROM employee;

SELECT name, 
depart_title['Product'][0] AS product_col0, 
depart_title['Test'][0] AS test_col0 
FROM employee;

3) Hive Table DDL :

Hive Table DDL

--Create internal table and load the data
CREATE TABLE IF NOT EXISTS employee_internal 
(
  name string,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
COMMENT 'This is an internal table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/osboxes/scripts/employee.txt' OVERWRITE INTO TABLE employee_internal;







--Create external table and load the data
CREATE EXTERNAL TABLE IF NOT EXISTS employee_external
 (
   name string,
   work_place ARRAY<string>,
   gender_age STRUCT<gender:string,age:int>,
   skills_score MAP<string,int>,
   depart_title MAP<STRING,ARRAY<STRING>>
 )
COMMENT 'This is an external table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE
LOCATION '/employee_external';

LOAD DATA LOCAL INPATH '/home/osboxes/scripts/employee.txt' OVERWRITE INTO TABLE employee_external;




--Create Table With Data - CREATE TABLE AS SELECT (CTAS)
CREATE TABLE ctas_employee AS SELECT * FROM employee_external;

--Create Table As SELECT (CTAS) with Common Table Expression (CTE) 
CREATE TABLE cte_employee AS
WITH r1 AS (SELECT name FROM r2 WHERE name = 'Michael'),
r2 AS (SELECT name FROM employee WHERE gender_age.gender= 'Male'),
r3 AS (SELECT name FROM employee WHERE gender_age.gender= 'Female')
SELECT * FROM r1 UNION ALL select * FROM r3;

SELECT * FROM cte_employee;





--Create Table Without Data - TWO ways 
--With CTAS
CREATE TABLE empty_ctas_employee AS SELECT * FROM employee_internal WHERE 1=2;

--With LIKE
CREATE TABLE empty_like_employee LIKE employee_internal;

--Check row count for both tables
SELECT COUNT(*) AS row_cnt FROM empty_ctas_employee;
SELECT COUNT(*) AS row_cnt FROM empty_like_employee;



--Drop table 
DROP TABLE IF EXISTS empty_ctas_employee;

DROP TABLE IF EXISTS empty_like_employee;



--Truncate table
SELECT * FROM cte_employee;

TRUNCATE TABLE cte_employee;

SELECT * FROM cte_employee;

--Alter table statements
--Alter table name
ALTER TABLE cte_employee RENAME TO c_employee;

--Alter table properties, such as comments
ALTER TABLE c_employee SET TBLPROPERTIES ('comment' = 'New name with new comments');

--Alter table delimiter through SerDe properties
ALTER TABLE employee_internal SET SERDEPROPERTIES ('field.delim' = '$');

--Alter Table File Format
ALTER TABLE c_employee SET FILEFORMAT RCFILE;

--Alter Table Location
ALTER TABLE c_employee SET LOCATION 'hdfs://localhost:8020/user/dayongd/employee'; 

--Alter Table Location
ALTER TABLE c_employee ENABLE NO_DROP; 
ALTER TABLE c_employee DISABLE NO_DROP; 
ALTER TABLE c_employee ENABLE OFFLINE;
ALTER TABLE c_employee DISABLE OFFLINE;

--Alter Table Concatenate to merge small files into larger files
--convert to the file format supported
ALTER TABLE c_employee SET FILEFORMAT ORC;
 
--concatenate files
ALTER TABLE c_employee CONCATENATE;

--convert to the regular file format
ALTER TABLE c_employee SET FILEFORMAT TEXTFILE;


--Alter columns
--Change column type - before changes
DESC employee_internal; 

--Change column type
ALTER TABLE employee_internal CHANGE name employee_name string AFTER gender_age;

--Verify the changes 
DESC employee_internal; 

--Change column type
ALTER TABLE employee_internal CHANGE employee_name name string FIRST;

--Verify the changes 
DESC employee_internal; 

--Add/Replace Columns-before add
DESC c_employee;      

--Add columns to the table
ALTER TABLE c_employee ADD COLUMNS (work string);

--Verify the added columns
DESC c_employee;      

--Replace all columns
ALTER TABLE c_employee REPLACE COLUMNS (name string);

--Verify the replaced all columns
DESC c_employee;   


4)HIve Partition bucket DDL :

 Hive Partition and Buckets DDL

--Create partition table DDL
CREATE TABLE employee_partitioned
(
  name string,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
PARTITIONED BY (Year INT, Month INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

--Show partitions
SHOW PARTITIONS employee_partitioned;

--Add multiple partitions
ALTER TABLE employee_partitioned ADD 
PARTITION (year=2014, month=11)        
PARTITION (year=2014, month=12);

SHOW PARTITIONS employee_partitioned;

ALTER TABLE employee_partitioned DROP PARTITION (year=2014, month=11);

SHOW PARTITIONS employee_partitioned;

--Load data to the partition
LOAD DATA LOCAL INPATH '/home/dayongd/Downloads/employee.txt' 
OVERWRITE INTO TABLE employee_partitioned
PARTITION (year=2014, month=12);

--Verify data loaded
SELECT name, year, month FROM employee_partitioned;







--Create a table with bucketing
--Prepare data for backet tables
CREATE TABLE employee_id                         
(
  name string,
  employee_id int,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

LOAD DATA LOCAL INPATH '/home/hadoop/Music/Classes/TrainingMaterials_InventTeq/SET4-hive/scripts/1Hive-DDL/employee_data_with_id.txt' 
OVERWRITE INTO TABLE employee_id

--Create the bucket table
drop table employee_id_buckets;
CREATE TABLE employee_id_buckets                         
(
  name string,
  employee_id int,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
CLUSTERED BY (employee_id) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';


------------------------------------------
set map.reduce.tasks = 2;

set mapreduce.job.reduces=2;

set hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE employee_id_buckets SELECT * FROM employee_id;



5) Hive View DDL :

 Hive View DDL

--Create Hive view
CREATE VIEW employee_skills
AS
SELECT name, skills_score['DB'] AS DB,
skills_score['Perl'] AS Perl, skills_score['Python'] AS Python,
skills_score['Sales'] as Sales, skills_score['HR'] as HR 
FROM employee;

--Alter views properties
ALTER VIEW employee_skills SET TBLPROPERTIES ('comment' = 'This is a view');

--Redefine views
ALTER VIEW employee_skills AS SELECT * from employee ;

--Drop views
DROP VIEW employee_skills; 




