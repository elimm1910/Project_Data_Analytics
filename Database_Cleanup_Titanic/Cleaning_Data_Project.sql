/*
Cleaning data using PotgreSQL and pgAdmin 4

In this project I am going to show how to clean and preprocess data to 
improve the quality and usability of a database taken from 
[https://www.kaggle.com/datasets/sakshisatre/titanic-dataset]. 
The objective of the project is to identify and correct erroneous, 
incomplete or duplicate data, ensuring the integrity and coherence 
of the information.
*/

-- Let's go... Create table and insert the database -------------------------

-- This table has two headers, so setting HEADER to true is not sufficient to 
-- remove them. Therefore, we will create a temporary table with TEXT data type 
-- to store the database with its headers, and then we will copy its data to 
-- another table excluding the headers 

CREATE TEMP TABLE Titanic_original (
	sn TEXT,
	pclass TEXT,
	survived TEXT,
	"name" TEXT,
	gender TEXT,
	age TEXT,
	"family" TEXT,
	fare TEXT,
	embarked TEXT,
	date TEXT
);

-- Copy values from database to temporary table
COPY Titanic_original
FROM '/path/to/database/The Titanic dataset.csv'
DELIMITER ','
CSV;

-- Verify that data was copied correctly
SELECT *
FROM Titanic_original;

-- Creating table with columns of correct type for the data to insert next
DROP TABLE IF EXISTS Titanic;
CREATE TABLE Titanic (
	sn INT,
	pclass INT,
	survived INT,
	name VARCHAR(100),
	gender VARCHAR(50),
	age TEXT,
	family INT,
	fare TEXT,
	embarked VARCHAR(50),
	date VARCHAR(50)
);

-- Insert data from temporary table excluding headers
INSERT INTO Titanic (sn, 
	pclass, survived, name, gender, age, family, fare, embarked, date)
SELECT CAST(sn AS INT),
	CAST(pclass AS INT),
	CAST(survived AS INT),
    name,
	gender,
	age,
	CAST(family AS INT),
	fare,
    embarked,
    date
FROM (
    SELECT *,
           ROW_NUMBER() OVER () AS rn
    FROM Titanic_original
) sub
WHERE rn > 2;

-- Verify that data was insert correctly
SELECT *
FROM Titanic;

-- Now let's start cleaning our database by following these steps
-- 1. Standardizethe Data
-- 2. Null values or blank values
-- 3. Remove Duplicates
-- 4. Remove Any Columns

-- Step 1. Standardizethe Data -----------------------------------------------

-- After analyzing the database we observed that there are some incorrect values 
-- such as the symbol "?" in the age column and "**" in the fare column. 
-- We set these values to null.

-- Let's check
SELECT NULLIF(REGEXP_REPLACE(fare, '[^a-zA-Z0-9 .]', '', 'g'), '') as fare_modify,
	fare,
	NULLIF(REGEXP_REPLACE(age, '[^a-zA-Z0-9 .]', '', 'g'), '') as age_modify,
	age
FROM Titanic
WHERE fare ~ '[^a-zA-Z0-9 .]' OR age ~ '[^a-zA-Z0-9 .]';

-- Let's edit these values
UPDATE Titanic
SET fare = NULLIF(REGEXP_REPLACE(fare, '[^a-zA-Z0-9 .]', '', 'g'), ''),
	age = NULLIF(REGEXP_REPLACE(age, '[^a-zA-Z0-9 .]', '', 'g'), '')
WHERE fare ~ '[^a-zA-Z0-9 .]' OR age ~ '[^a-zA-Z0-9 .]';

-- Now we can see that there are some incorrect age values, with floating point 
-- values like 0.83. The presence of outliers can distort statistical analyzes 
-- that depend on the age distribution, so we should set these values to null

-- Detect floating point values
SELECT age
FROM Titanic
WHERE age ~ '^\d+\.\d+$';

-- Set these values to NULL
UPDATE Titanic
SET age = NULL
WHERE age ~ '^\d+\.\d+$';

-- Then we can convert the data type of the age field to INT
ALTER TABLE Titanic
ALTER COLUMN age
SET DATA TYPE INT
USING age::INT

-- We can also convert the data type of the fare field to NUMERIC
ALTER TABLE Titanic
ALTER COLUMN fare
SET DATA TYPE NUMERIC(7,4)
USING fare::NUMERIC(7,4)

-- Another necessary data change is to change the date from string to DATE
ALTER TABLE Titanic
ALTER COLUMN date
SET DATA TYPE DATE
USING date::DATE; 

-- Step 1. Completed ---------------------------------------------------------

-- Step 2. Null values or blank values ---------------------------------------

-- Now let's analyze the data to see how we can populate some of the empty 
-- or null spaces
SELECT *
FROM Titanic
WHERE gender isNULL OR gender = '';

-- We see that we have a null gender field that can be filled from the name of 
-- the person, if it is Mr. it would correspond to male, otherwise it would 
-- correspond to famele

-- Verify the convertion
SELECT name, gender, 
	CASE  
	WHEN name LIKE '%Mr.%' THEN 'male'
	ELSE 'famale'
	END AS modify_gender
FROM Titanic
WHERE gender isNULL OR gender = '';

-- Complete the null or empty gender 
UPDATE Titanic
SET gender = CASE  
	WHEN name LIKE '%Mr.%' THEN 'male'
	ELSE 'famale'
	END
WHERE gender isNULL OR gender = '';

-- Now let's delete the rows that contain two or more null values

-- Detect all rows that containt two or more null values
SELECT *
FROM Titanic
WHERE (
	CASE WHEN age is Null THEN 1 ELSE 0 END + 
	CASE WHEN family is Null THEN 1 ELSE 0 END +
	CASE WHEN fare is Null THEN 1 ELSE 0 END +
	CASE WHEN embarked is Null THEN 1 ELSE 0 END) >= 2;

-- Delete these
DELETE 
FROM Titanic
WHERE (
	CASE WHEN age is Null THEN 1 ELSE 0 END + 
	CASE WHEN family is Null THEN 1 ELSE 0 END +
	CASE WHEN fare is Null THEN 1 ELSE 0 END +
	CASE WHEN embarked is Null THEN 1 ELSE 0 END) >= 2;

-- Populate the missing ages with the mean of these

-- Check for null values of age and average.
SELECT age, 
	(SELECT ROUND(AVG(age))
	FROM Titanic) AS avg_age
FROM Titanic
WHERE age is NULL;

-- Update the values
UPDATE Titanic
SET age = (SELECT ROUND(AVG(age))
	FROM Titanic)
WHERE age is NULL;

-- Now we are goin to edit sn fils and put consecutives numbers like 

-- Step 2. Completed ---------------------------------------------------------

-- Step 3. Remove Duplicates -------------------------------------------------

-- We are going to detect if there is a repeated field, 
-- if row_num > 1 the field is repeated
WITH duplicate AS
(
SELECT ctid, *,
ROW_NUMBER() OVER(
PARTITION BY sn,
pclass, 
survived, 
name, 
gender,
age,
family,
fare,
embarked,
date) AS row_num
FROM Titanic
)

-- This is for see the duplicate value
SELECT *
FROM duplicate
WHERE row_num > 1;

-- We detect a repeated field, let's delete them
DELETE FROM Titanic
WHERE ctid IN (
	SELECT ctid
	FROM duplicate
	WHERE row_num > 1
);

-- Step 3. Completed ----------------------------------------------------------
	
-- Step 4. Remove Any Columns--------------------------------------------------

-- In this step it is necessary to analyze if all the fields in my table are 
-- necessary and provide me with unique and important information. In this case 
-- I think that the sn field, which is a unique repetitive number, does not 
-- give me the necessary information, for that reason we are going to eliminate 
-- this field. This is a consideration, in many cases it is important to keep 
-- this field as an identifier.

ALTER TABLE Titanic
DROP COLUMN sn;

-- Step 4. Completed-----------------------------------------------------------

-- See the final result
SELECT *
FROM Titanic;

-- Export table
COPY Titanic TO '/path/save/database/Titanic_clean.csv' WITH (FORMAT CSV, HEADER);
