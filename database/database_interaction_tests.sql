USE courseleaf_dev
GO
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
-- Test code for CourseLeafApp database trigger/SP interactions
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

/*
Tests are designed to be run in sequence. Each test case works by checking the state of the relevant
_current table, which will maintain its state in between test cases.

To run the tests, run all of the code from the start of the test suite until the first GO statement.
Check the results of the query that is at the end of each batch to confirm that there are no 
differences between the test dataset and the state of the relevant _current table.
*/


---------------------------------------------------------------------------------------------------------
-- Testing update_current_courses
---------------------------------------------------------------------------------------------------------

TRUNCATE TABLE banner_courses;
TRUNCATE TABLE current_courses;
TRUNCATE TABLE imports;

-- 1. Testing initial insert with data from multiple terms.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 1, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id()

INSERT INTO
	banner_courses (load_id, term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status, insert_timestamp)
VALUES
	(@LOADID, '120258', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_courses;

DROP TABLE IF EXISTS #courses_test_1;
CREATE TABLE #courses_test_1 (
	term nvarchar(6),
	course nvarchar(50),
	subject_code nvarchar(50),
	course_no nvarchar(3),
	course_title nvarchar(255),
	college nvarchar(2),
	dept_no nvarchar(4),
	control_code nvarchar(1),
	course_id nvarchar(7),
	course_start_term nvarchar(6),
	course_end_term nvarchar(6),
	course_effective_term nvarchar(6),
	status nvarchar(1)
);
INSERT INTO #courses_test_1
VALUES
	('120258', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A'),
	('120258', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A'),
	('120258', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A'),
	('120258', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A'),
	('120258', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A'),
	('120258', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A'),
	('120258', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A'),
	('120258', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120228', 'A'),
	('120258', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A'),
	('120258', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A'),
	('120261', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A'),
	('120261', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A'),
	('120261', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A'),
	('120261', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A'),
	('120261', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A'),
	('120261', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A'),
	('120261', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A'),
	('120261', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120228', 'A'),
	('120261', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A'),
	('120261', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A')
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status
FROM
	current_courses
EXCEPT
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status
FROM
	#courses_test_1
)
UNION
(
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status
FROM
	#courses_test_1
EXCEPT
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status
FROM
	current_courses
)
;
GO

-- 2. Testing that current_courses does not change when same data inserted

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 2, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id()

INSERT INTO
	banner_courses (load_id, term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status, insert_timestamp)
VALUES
	(@LOADID, '120258', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_courses;

DROP TABLE IF EXISTS #courses_test_2;
SELECT
	*
INTO
	#courses_test_2
FROM
	#courses_test_1
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status
FROM
	current_courses
EXCEPT
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status
FROM
	#courses_test_2
)
UNION
(
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status
FROM
	#courses_test_2
EXCEPT
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status
FROM
	current_courses
)
;
GO

-- 3. Testing insert where effective term of courses changes

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 3, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id()

-- Effective term updates:
-- 120228 has been decreased to 120208
-- 120258 has been increased to 120261
INSERT INTO
	banner_courses (load_id, term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status, insert_timestamp)
VALUES
	(@LOADID, '120258', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120208', 'A', @DATE),
	(@LOADID, '120258', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120208', 'A', @DATE),
	(@LOADID, '120258', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120261', 'A', @DATE),
	(@LOADID, '120258', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120261', 'A', @DATE),
	(@LOADID, '120258', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120261', 'A', @DATE),
	(@LOADID, '120258', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120261', 'A', @DATE),
	(@LOADID, '120258', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120261', 'A', @DATE),
	(@LOADID, '120258', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120208', 'A', @DATE),
	(@LOADID, '120258', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120208', 'A', @DATE),
	(@LOADID, '120258', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120208', 'A', @DATE),
	(@LOADID, '120261', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120208', 'A', @DATE),
	(@LOADID, '120261', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120208', 'A', @DATE),
	(@LOADID, '120261', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120261', 'A', @DATE),
	(@LOADID, '120261', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120261', 'A', @DATE),
	(@LOADID, '120261', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120261', 'A', @DATE),
	(@LOADID, '120261', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120261', 'A', @DATE),
	(@LOADID, '120261', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120261', 'A', @DATE),
	(@LOADID, '120261', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120208', 'A', @DATE),
	(@LOADID, '120261', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120208', 'A', @DATE),
	(@LOADID, '120261', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120208', 'A', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_courses;

DROP TABLE IF EXISTS #courses_test_3;
SELECT
	*
INTO
	#courses_test_3
FROM
	#courses_test_1
;
-- The only change in the current_courses table should be the effective term being updated to the future; backdating shouldn't do anything.
UPDATE
	#courses_test_3
SET
	course_effective_term = '120261'
WHERE
	course_effective_term = '120258'
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status
FROM
	current_courses
EXCEPT
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status
FROM
	#courses_test_3
)
UNION
(
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status
FROM
	#courses_test_3
EXCEPT
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status
FROM
	current_courses
)
;
GO

---------------------------------------------------------------------------------------------------------
-- Testing update_current_crosslists
---------------------------------------------------------------------------------------------------------

TRUNCATE TABLE banner_courses;
TRUNCATE TABLE current_courses;
TRUNCATE TABLE imports;

-- 1. Testing initial insert of both crosslisted and non-crosslisted courses.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 1, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_courses (load_id, term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status, insert_timestamp)
VALUES
	(@LOADID, '120258', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_courses;
EXEC courseleaf_dev.dbo.update_current_crosslists;

DROP TABLE IF EXISTS #crosslists_test_1;
CREATE TABLE #crosslists_test_1 (
	crosslist_effective_term nvarchar(6),
	course nvarchar(50),
	controlling_course nvarchar(50),
	controlling_dept_no nvarchar(4),
	is_crosslisted nvarchar(1),
	is_controlling nvarchar(1)
);
INSERT INTO #crosslists_test_1
VALUES
	('120258', 'AE 199', 'AE 199', '1615', 'N', 'Y'),
	('120258', 'ACCY 410', 'ACCY 410', '1346', 'N', 'Y'),
	('120258', 'ACE 210', 'ACE 210', '1470', 'Y', 'Y'),
	('120258', 'ENVS 210', 'ACE 210', '1470', 'Y', 'N'),
	('120258', 'NRES 210', 'ACE 210', '1470', 'Y', 'N'),
	('120258', 'UP 210', 'ACE 210', '1470', 'Y', 'N'),
	('120258', 'ECON 210', 'ACE 210', '1470', 'Y', 'N'),
	('120258', 'AAS 258', 'AAS 258', '1404', 'Y', 'Y'),
	('120258', 'LLS 258', 'AAS 258', '1404', 'Y', 'N'),
	('120258', 'REL 258', 'AAS 258', '1404', 'Y', 'N'),
	('120261', 'AE 199', 'AE 199', '1615', 'N', 'Y'),
	('120261', 'ACCY 410', 'ACCY 410', '1346', 'N', 'Y'),
	('120261', 'ACE 210', 'ACE 210', '1470', 'Y', 'Y'),
	('120261', 'ENVS 210', 'ACE 210', '1470', 'Y', 'N'),
	('120261', 'NRES 210', 'ACE 210', '1470', 'Y', 'N'),
	('120261', 'UP 210', 'ACE 210', '1470', 'Y', 'N'),
	('120261', 'ECON 210', 'ACE 210', '1470', 'Y', 'N'),
	('120261', 'AAS 258', 'AAS 258', '1404', 'Y', 'Y'),
	('120261', 'LLS 258', 'AAS 258', '1404', 'Y', 'N'),
	('120261', 'REL 258', 'AAS 258', '1404', 'Y', 'N')
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling
FROM
	current_crosslists
EXCEPT
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling
FROM
	#crosslists_test_1
)
UNION
(
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling
FROM
	#crosslists_test_1
EXCEPT
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling
FROM
	current_crosslists
)
;
GO

-- 2. Testing re-insert of identical data.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 2, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_courses (load_id, term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status, insert_timestamp)
VALUES
	(@LOADID, '120258', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120258', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120258', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120261', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120261', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_courses;
EXEC courseleaf_dev.dbo.update_current_crosslists;

DROP TABLE IF EXISTS #crosslists_test_2;
SELECT
	*
INTO
	#crosslists_test_2
FROM
	#crosslists_test_1
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling
FROM
	current_crosslists
EXCEPT
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling
FROM
	#crosslists_test_2
)
UNION
(
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling
FROM
	#crosslists_test_2
EXCEPT
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling
FROM
	current_crosslists
)
;
GO

-- 3. Testing insert of data with courses from additional terms.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 3, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_courses (load_id, term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status, insert_timestamp)
VALUES
	(@LOADID, '120268', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120268', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120268', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120268', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120268', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120268', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120268', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120268', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120268', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120268', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120271', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120271', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A', @DATE),
	(@LOADID, '120271', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120271', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120271', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120271', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120271', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', @DATE),
	(@LOADID, '120271', 'AAS 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'C', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120271', 'LLS 258', 'LLS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE),
	(@LOADID, '120271', 'REL 258', 'AAS', '258', 'Muslims in America', 'KV', '1404', 'N', '1009522', '120091', '999999', '120228', 'A', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_courses;
EXEC courseleaf_dev.dbo.update_current_crosslists;

DROP TABLE IF EXISTS #crosslists_test_3;
SELECT
	*
INTO
	#crosslists_test_3
FROM
	#crosslists_test_1
;
INSERT INTO #crosslists_test_3
VALUES
	('120268', 'AE 199', 'AE 199', '1615', 'N', 'Y'),
	('120268', 'ACCY 410', 'ACCY 410', '1346', 'N', 'Y'),
	('120268', 'ACE 210', 'ACE 210', '1470', 'Y', 'Y'),
	('120268', 'ENVS 210', 'ACE 210', '1470', 'Y', 'N'),
	('120268', 'NRES 210', 'ACE 210', '1470', 'Y', 'N'),
	('120268', 'UP 210', 'ACE 210', '1470', 'Y', 'N'),
	('120268', 'ECON 210', 'ACE 210', '1470', 'Y', 'N'),
	('120268', 'AAS 258', 'AAS 258', '1404', 'Y', 'Y'),
	('120268', 'LLS 258', 'AAS 258', '1404', 'Y', 'N'),
	('120268', 'REL 258', 'AAS 258', '1404', 'Y', 'N'),
	('120271', 'AE 199', 'AE 199', '1615', 'N', 'Y'),
	('120271', 'ACCY 410', 'ACCY 410', '1346', 'N', 'Y'),
	('120271', 'ACE 210', 'ACE 210', '1470', 'Y', 'Y'),
	('120271', 'ENVS 210', 'ACE 210', '1470', 'Y', 'N'),
	('120271', 'NRES 210', 'ACE 210', '1470', 'Y', 'N'),
	('120271', 'UP 210', 'ACE 210', '1470', 'Y', 'N'),
	('120271', 'ECON 210', 'ACE 210', '1470', 'Y', 'N'),
	('120271', 'AAS 258', 'AAS 258', '1404', 'Y', 'Y'),
	('120271', 'LLS 258', 'AAS 258', '1404', 'Y', 'N'),
	('120271', 'REL 258', 'AAS 258', '1404', 'Y', 'N')
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling
FROM
	current_crosslists
EXCEPT
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling
FROM
	#crosslists_test_3
)
UNION
(
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling
FROM
	#crosslists_test_3
EXCEPT
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling
FROM
	current_crosslists
)
;
GO

---------------------------------------------------------------------------------------------------------
-- Testing update_current_departments
---------------------------------------------------------------------------------------------------------

TRUNCATE TABLE banner_departments;
TRUNCATE TABLE current_departments;
TRUNCATE TABLE imports;

-- 1. Testing initial insert.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 1, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_departments (load_id, dept_no, dept_name, insert_timestamp)
VALUES
	(@LOADID, '1211', 'CIC Traveling Scholars', @DATE),
	(@LOADID, '1230', 'MBA Program Administration', @DATE),
	(@LOADID, '1241', 'Anthropology', @DATE),
	(@LOADID, '1244', 'Physics', @DATE),
	(@LOADID, '1246', 'Computational Science & Engr', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_departments;

DROP TABLE IF EXISTS #departments_test_1;
CREATE TABLE #departments_test_1 (
	dept_no nvarchar(4),
	dept_name nvarchar(255)
);
INSERT INTO #departments_test_1
VALUES
	('1211', 'CIC Traveling Scholars'),
	('1230', 'MBA Program Administration'),
	('1241', 'Anthropology'),
	('1244', 'Physics'),
	('1246', 'Computational Science & Engr')
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	dept_no, dept_name
FROM
	current_departments
EXCEPT
SELECT
	dept_no, dept_name
FROM
	#departments_test_1
)
UNION
(
SELECT
	dept_no, dept_name
FROM
	#departments_test_1
EXCEPT
SELECT
	dept_no, dept_name
FROM
	current_departments
)
;
GO

-- 2. Testing insert of identical data.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 2, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_departments (load_id, dept_no, dept_name, insert_timestamp)
VALUES
	(@LOADID, '1211', 'CIC Traveling Scholars', @DATE),
	(@LOADID, '1230', 'MBA Program Administration', @DATE),
	(@LOADID, '1241', 'Anthropology', @DATE),
	(@LOADID, '1244', 'Physics', @DATE),
	(@LOADID, '1246', 'Computational Science & Engr', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_departments;

DROP TABLE IF EXISTS #departments_test_2;
SELECT
	*
INTO
	#departments_test_2
FROM
	#departments_test_1
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	dept_no, dept_name
FROM
	current_departments
EXCEPT
SELECT
	dept_no, dept_name
FROM
	#departments_test_2
)
UNION
(
SELECT
	dept_no, dept_name
FROM
	#departments_test_2
EXCEPT
SELECT
	dept_no, dept_name
FROM
	current_departments
)
;
GO

-- 3. Testing insert with additional departments.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 3, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_departments (load_id, dept_no, dept_name, insert_timestamp)
VALUES
	(@LOADID, '1211', 'CIC Traveling Scholars', @DATE),
	(@LOADID, '1230', 'MBA Program Administration', @DATE),
	(@LOADID, '1241', 'Anthropology', @DATE),
	(@LOADID, '1244', 'Physics', @DATE),
	(@LOADID, '1246', 'Computational Science & Engr', @DATE),
	(@LOADID, '1257', 'Mathematics', @DATE),
	(@LOADID, '1260', 'Finance', @DATE),
	(@LOADID, '1282', 'Pathobiology', @DATE),
	(@LOADID, '1361', 'Entomology', @DATE),
	(@LOADID, '1413', 'Chemistry', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_departments;

DROP TABLE IF EXISTS #departments_test_3;
SELECT
	*
INTO
	#departments_test_3
FROM
	#departments_test_1
;
INSERT INTO #departments_test_3
VALUES
	('1257', 'Mathematics'),
	('1260', 'Finance'),
	('1282', 'Pathobiology'),
	('1361', 'Entomology'),
	('1413', 'Chemistry')
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	dept_no, dept_name
FROM
	current_departments
EXCEPT
SELECT
	dept_no, dept_name
FROM
	#departments_test_3
)
UNION
(
SELECT
	dept_no, dept_name
FROM
	#departments_test_3
EXCEPT
SELECT
	dept_no, dept_name
FROM
	current_departments
)
;
GO

-- 4. Testing insert with some department names changed.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 4, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_departments (load_id, dept_no, dept_name, insert_timestamp)
VALUES
	(@LOADID, '1211', 'CIC Traveling Scholars', @DATE),
	(@LOADID, '1230', 'MBA Program Administration', @DATE),
	(@LOADID, '1241', 'Anthropology', @DATE),
	(@LOADID, '1244', 'Physics', @DATE),
	(@LOADID, '1246', 'Computational Science & Engr', @DATE),
	(@LOADID, '1257', 'Mathematics but cooler', @DATE),
	(@LOADID, '1260', 'Finance', @DATE),
	(@LOADID, '1282', 'Pathobiology', @DATE),
	(@LOADID, '1361', 'Entomology', @DATE),
	(@LOADID, '1413', 'Chemistry', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_departments;

DROP TABLE IF EXISTS #departments_test_4;
SELECT
	*
INTO
	#departments_test_4
FROM
	#departments_test_3
;
UPDATE
	#departments_test_4
SET
	dept_name = 'Mathematics but cooler'
WHERE
	dept_no = '1257'
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	dept_no, dept_name
FROM
	current_departments
EXCEPT
SELECT
	dept_no, dept_name
FROM
	#departments_test_4
)
UNION
(
SELECT
	dept_no, dept_name
FROM
	#departments_test_4
EXCEPT
SELECT
	dept_no, dept_name
FROM
	current_departments
)
;
GO

-- 5. Testing insert with dropped departments.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 5, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_departments (load_id, dept_no, dept_name, insert_timestamp)
VALUES
	(@LOADID, '1211', 'CIC Traveling Scholars', @DATE),
	(@LOADID, '1230', 'MBA Program Administration', @DATE),
	(@LOADID, '1241', 'Anthropology', @DATE),
	(@LOADID, '1244', 'Physics', @DATE),
	(@LOADID, '1246', 'Computational Science & Engr', @DATE),
	-- Mathematics but cooler has been excluded
	(@LOADID, '1260', 'Finance', @DATE),
	(@LOADID, '1282', 'Pathobiology', @DATE),
	(@LOADID, '1361', 'Entomology', @DATE),
	(@LOADID, '1413', 'Chemistry', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_departments;

DROP TABLE IF EXISTS #departments_test_5;
SELECT
	*
INTO
	#departments_test_5
FROM
	#departments_test_4
;
DELETE FROM
	#departments_test_5
WHERE
	dept_no = '1257'
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	dept_no, dept_name
FROM
	current_departments
EXCEPT
SELECT
	dept_no, dept_name
FROM
	#departments_test_5
)
UNION
(
SELECT
	dept_no, dept_name
FROM
	#departments_test_5
EXCEPT
SELECT
	dept_no, dept_name
FROM
	current_departments
)
;
GO

---------------------------------------------------------------------------------------------------------
-- Testing update_current_roles
---------------------------------------------------------------------------------------------------------

TRUNCATE TABLE courseleaf_roles;
TRUNCATE TABLE current_roles;
TRUNCATE TABLE imports;
GO

-- 1. Testing basic insert.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 1, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	courseleaf_roles (load_id, role, dept_no, dept, role_title, uin, insert_timestamp)
VALUES
	(@LOADID, '1211-CIC Head', '1211', 'CIC', 'Head', '675086358', @DATE),
	(@LOADID, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656120313', @DATE),
	(@LOADID, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656058033', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_roles;

DROP TABLE IF EXISTS #roles_test_1;
CREATE TABLE #roles_test_1 (
	uin nvarchar(9),
	role nvarchar(50),
	sequence_number int,
	dept_no nvarchar(4),
	dept nvarchar(50),
	role_title nvarchar(50),
	role_begin_date date,
	role_end_date date
);
INSERT INTO #roles_test_1
	(uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date)
VALUES
	('675086358', '1211-CIC Head', 1, '1211', 'CIC', 'Head', CAST(GETDATE() AS DATE), NULL), -- Date should always be the date on which the test is run
	('656120313', '1241-ANTH Head', 1, '1241', 'ANTH', 'Head', CAST(GETDATE() AS DATE), NULL),
	('656058033', '1241-ANTH Head', 1, '1241', 'ANTH', 'Head', CAST(GETDATE() AS DATE), NULL)
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	current_roles
EXCEPT
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	#roles_test_1
)
UNION
(
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	#roles_test_1
EXCEPT
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	current_roles
)
;
GO

-- 2. Testing ending a role.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 2, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	courseleaf_roles (load_id, role, dept_no, dept, role_title, uin, insert_timestamp)
VALUES
	(@LOADID, '1211-CIC Head', '1211', 'CIC', 'Head', '658725029', @DATE), -- Adding a different UIN as CIC head
	(@LOADID, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656120313', @DATE),
	(@LOADID, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656058033', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_roles;

DROP TABLE IF EXISTS #roles_test_2;
SELECT
	*
INTO
	#roles_test_2
FROM
	#roles_test_1
;
INSERT INTO
	#roles_test_2 (uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date)
VALUES
	('658725029', '1211-CIC Head', 1, '1211', 'CIC', 'Head', CAST(GETDATE() AS DATE), NULL)
;
UPDATE
	#roles_test_2
SET
	role_end_date = CAST(GETDATE() AS DATE)
WHERE
	uin = '675086358'
	AND role = '1211-CIC Head'
	AND sequence_number = 1
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	current_roles
EXCEPT
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	#roles_test_2
)
UNION
(
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	#roles_test_2
EXCEPT
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	current_roles
)
;
GO

-- 3. Testing re-starting a previously ended role.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 3, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	courseleaf_roles (load_id, role, dept_no, dept, role_title, uin, insert_timestamp)
VALUES
	(@LOADID, '1211-CIC Head', '1211', 'CIC', 'Head', '658725029', @DATE), -- this UIN already has the role
	(@LOADID, '1211-CIC Head', '1211', 'CIC', 'Head', '675086358', @DATE), -- this UIN previously had the role, which has since been deactivated
	(@LOADID, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656120313', @DATE),
	(@LOADID, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656058033', @DATE)
;


EXEC courseleaf_dev.dbo.update_current_roles;

DROP TABLE IF EXISTS #roles_test_3;
SELECT
	*
INTO
	#roles_test_3
FROM
	#roles_test_2
;
INSERT INTO
	#roles_test_3 (uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date)
VALUES
	('675086358', '1211-CIC Head', 2, '1211', 'CIC', 'Head', CAST(GETDATE() AS DATE), NULL)
;
-- Test succeeds if this query returns 0 rows
(
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	current_roles
EXCEPT
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	#roles_test_3
)
UNION
(
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	#roles_test_3
EXCEPT
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	current_roles
)
;
GO

-- 4. Testing case where an individual leaves one role and begins a different role simultaneously.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 4, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	courseleaf_roles (load_id, role, dept_no, dept, role_title, uin, insert_timestamp)
VALUES
	(@LOADID, '1211-CIC Head', '1211', 'CIC', 'Head', '675086358', @DATE),
	(@LOADID, '1248-ES Head', '1248', 'ES', 'Head', '658725029', @DATE), -- this UIN previously had the role of CIC Head and is now moving to ES Head
	(@LOADID, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656120313', @DATE),
	(@LOADID, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656058033', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_roles;

DROP TABLE IF EXISTS #roles_test_4;
SELECT
	*
INTO
	#roles_test_4
FROM
	#roles_test_3
;
INSERT INTO
	#roles_test_4 (uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date)
VALUES
	('658725029', '1248-ES Head', 1, '1248', 'ES', 'Head', CAST(GETDATE() AS DATE), NULL)
;
UPDATE
	#roles_test_4
SET
	role_end_date = CAST(GETDATE() AS DATE)
WHERE
	uin = '658725029'
	AND role = '1211-CIC Head'
	AND sequence_number = 1
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	current_roles
EXCEPT
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	#roles_test_4
)
UNION
(
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	#roles_test_4
EXCEPT
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	current_roles
)
;
GO

-- 5. Testing ending and then re-starting ALL roles.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 5, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

-- Running role update without inserting any rows in most recent data load should end all active roles.
EXEC courseleaf_dev.dbo.update_current_roles;
GO

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 6, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	courseleaf_roles (load_id, role, dept_no, dept, role_title, uin, insert_timestamp)
VALUES
	(@LOADID, '1211-CIC Head', '1211', 'CIC', 'Head', '675086358', @DATE),
	(@LOADID, '1248-ES Head', '1248', 'ES', 'Head', '658725029', @DATE),
	(@LOADID, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656120313', @DATE),
	(@LOADID, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656058033', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_roles;

DROP TABLE IF EXISTS #roles_test_5;
SELECT
	*
INTO
	#roles_test_5
FROM
	#roles_test_4
;
UPDATE
	#roles_test_5
SET
	role_end_date = CAST(GETDATE() AS DATE)
;
INSERT INTO
	#roles_test_5 (uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date)
VALUES
	('675086358', '1211-CIC Head', 3, '1211', 'CIC', 'Head', CAST(GETDATE() AS DATE), NULL),
	('658725029', '1248-ES Head', 2, '1248', 'ES', 'Head', CAST(GETDATE() AS DATE), NULL),
	('656120313', '1241-ANTH Head', 2, '1241', 'ANTH', 'Head', CAST(GETDATE() AS DATE), NULL),
	('656058033', '1241-ANTH Head', 2, '1241', 'ANTH', 'Head', CAST(GETDATE() AS DATE), NULL)
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	current_roles
EXCEPT
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	#roles_test_5
)
UNION
(
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	#roles_test_5
EXCEPT
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date
FROM
	current_roles
)
;
GO

---------------------------------------------------------------------------------------------------------
-- Testing update_current_subjects
---------------------------------------------------------------------------------------------------------

TRUNCATE TABLE banner_subjects;
TRUNCATE TABLE current_subjects;
TRUNCATE TABLE imports;

-- 1. Testing initial insert.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 1, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_subjects (load_id, subject_code, subject, subject_name_codebook, insert_timestamp)
VALUES
	(@LOADID, 'AAS', 'Asian American Studies', 'Asian American Studies', @DATE),
	(@LOADID, 'ABE', 'Agric & Biological Engineering', 'Agricultural and Biological Engineering', @DATE),
	(@LOADID, 'ACCY', 'Accountancy', 'Accountancy', @DATE),
	(@LOADID, 'ACE', 'Agr & Consumer Economics', 'Agricultural and Consumer Economics', @DATE),
	(@LOADID, 'ACES', 'Agr, Consumer, & Env Sciences', 'Agricultural, Consumer and Environmental Sciences', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_subjects;

DROP TABLE IF EXISTS #subjects_test_1;
CREATE TABLE #subjects_test_1 (
	subject_code nvarchar(50),
	subject nvarchar(50),
	subject_name_codebook nvarchar(255)
);
INSERT INTO #subjects_test_1
VALUES
	('AAS', 'Asian American Studies', 'Asian American Studies'),
	('ABE', 'Agric & Biological Engineering', 'Agricultural and Biological Engineering'),
	('ACCY', 'Accountancy', 'Accountancy'),
	('ACE', 'Agr & Consumer Economics', 'Agricultural and Consumer Economics'),
	('ACES', 'Agr, Consumer, & Env Sciences', 'Agricultural, Consumer and Environmental Sciences')
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	subject_code, subject, subject_name_codebook
FROM
	current_subjects
EXCEPT
SELECT
	subject_code, subject, subject_name_codebook
FROM
	#subjects_test_1
)
UNION
(
SELECT
	subject_code, subject, subject_name_codebook
FROM
	#subjects_test_1
EXCEPT
SELECT
	subject_code, subject, subject_name_codebook
FROM
	current_subjects
)
;
GO

-- 2. Testing insert with additional departments.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 2, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_subjects (load_id, subject_code, subject, subject_name_codebook, insert_timestamp)
VALUES
	(@LOADID, 'AAS', 'Asian American Studies', 'Asian American Studies', @DATE),
	(@LOADID, 'ABE', 'Agric & Biological Engineering', 'Agricultural and Biological Engineering', @DATE),
	(@LOADID, 'ACCY', 'Accountancy', 'Accountancy', @DATE),
	(@LOADID, 'ACE', 'Agr & Consumer Economics', 'Agricultural and Consumer Economics', @DATE),
	(@LOADID, 'ACES', 'Agr, Consumer, & Env Sciences', 'Agricultural, Consumer and Environmental Sciences', @DATE),
	(@LOADID, 'BADM', 'Business Administration', 'Business Administration', @DATE),
	(@LOADID, 'BASQ', 'Basque', 'Basque', @DATE),
	(@LOADID, 'BCOG', 'Brain and Cognitive Science', 'Brain and Cognitive Science', @DATE),
	(@LOADID, 'BCS', 'Bosnian-Croatian-Serbian', 'Bosnian-Croatian-Serbian', @DATE),
	(@LOADID, 'BDI', 'Business Data and Innovation', 'Business Data and Innovation', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_subjects;

DROP TABLE IF EXISTS #subjects_test_2;
SELECT
	*
INTO
	#subjects_test_2
FROM
	#subjects_test_1
;
INSERT INTO #subjects_test_2
VALUES
	('BADM', 'Business Administration', 'Business Administration'),
	('BASQ', 'Basque', 'Basque'),
	('BCOG', 'Brain and Cognitive Science', 'Brain and Cognitive Science'),
	('BCS', 'Bosnian-Croatian-Serbian', 'Bosnian-Croatian-Serbian'),
	('BDI', 'Business Data and Innovation', 'Business Data and Innovation')
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	subject_code, subject, subject_name_codebook
FROM
	current_subjects
EXCEPT
SELECT
	subject_code, subject, subject_name_codebook
FROM
	#subjects_test_2
)
UNION
(
SELECT
	subject_code, subject, subject_name_codebook
FROM
	#subjects_test_2
EXCEPT
SELECT
	subject_code, subject, subject_name_codebook
FROM
	current_subjects
)
;
GO

-- 3. Testing insert with some department names changes.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 3, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_subjects (load_id, subject_code, subject, subject_name_codebook, insert_timestamp)
VALUES
	(@LOADID, 'AAS', 'Asian American Studies', 'Asian American Studies', @DATE),
	(@LOADID, 'ABE', 'Agric & Biological Engineering', 'Agricultural and Biological Engineering', @DATE),
	(@LOADID, 'ACCY', 'Accountancy but cooler', 'Accountancy', @DATE),
	(@LOADID, 'ACE', 'Agr & Consumer Economics', 'Agricultural and Consumer Economics', @DATE),
	(@LOADID, 'ACES', 'Agr, Consumer, & Env Sciences', 'Agricultural, Consumer and Environmental Sciences', @DATE),
	(@LOADID, 'BADM', 'Bad Man!', 'Business Administration', @DATE),
	(@LOADID, 'BASQ', 'Basque', 'Basque', @DATE),
	(@LOADID, 'BCOG', 'Brain and Cognitive Science', 'Brain and Cognitive Science', @DATE),
	(@LOADID, 'BCS', 'Bosnian-Croatian-Serbian', 'Bosnian-Croatian-Serbian', @DATE),
	(@LOADID, 'BDI', 'Business Data and Innovation', 'Business Data and Innovation', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_subjects;

DROP TABLE IF EXISTS #subjects_test_3;
SELECT
	*
INTO
	#subjects_test_3
FROM
	#subjects_test_2
;
UPDATE
	#subjects_test_3
SET
	subject = 'Accountancy but cooler'
WHERE
	subject_code = 'ACCY'
;
UPDATE
	#subjects_test_3
SET
	subject = 'Bad Man!'
WHERE
	subject_code = 'BADM'
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	subject_code, subject, subject_name_codebook
FROM
	current_subjects
EXCEPT
SELECT
	subject_code, subject, subject_name_codebook
FROM
	#subjects_test_3
)
UNION
(
SELECT
	subject_code, subject, subject_name_codebook
FROM
	#subjects_test_3
EXCEPT
SELECT
	subject_code, subject, subject_name_codebook
FROM
	current_subjects
)
;
GO

-- 4. Testing insert with missing departments for deletion.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 4, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_subjects (load_id, subject_code, subject, subject_name_codebook, insert_timestamp)
VALUES
	(@LOADID, 'AAS', 'Asian American Studies', 'Asian American Studies', @DATE),
	(@LOADID, 'ABE', 'Agric & Biological Engineering', 'Agricultural and Biological Engineering', @DATE),
	(@LOADID, 'ACE', 'Agr & Consumer Economics', 'Agricultural and Consumer Economics', @DATE),
	(@LOADID, 'ACES', 'Agr, Consumer, & Env Sciences', 'Agricultural, Consumer and Environmental Sciences', @DATE),
	(@LOADID, 'BASQ', 'Basque', 'Basque', @DATE),
	(@LOADID, 'BCOG', 'Brain and Cognitive Science', 'Brain and Cognitive Science', @DATE),
	(@LOADID, 'BCS', 'Bosnian-Croatian-Serbian', 'Bosnian-Croatian-Serbian', @DATE),
	(@LOADID, 'BDI', 'Business Data and Innovation', 'Business Data and Innovation', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_subjects;

DROP TABLE IF EXISTS #subjects_test_4;
SELECT
	*
INTO
	#subjects_test_4
FROM
	#subjects_test_3
;
DELETE
FROM
	#subjects_test_4
WHERE
	subject_code IN ('ACCY', 'BADM')
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	subject_code, subject, subject_name_codebook
FROM
	current_subjects
EXCEPT
SELECT
	subject_code, subject, subject_name_codebook
FROM
	#subjects_test_4
)
UNION
(
SELECT
	subject_code, subject, subject_name_codebook
FROM
	#subjects_test_4
EXCEPT
SELECT
	subject_code, subject, subject_name_codebook
FROM
	current_subjects
)
;
GO

---------------------------------------------------------------------------------------------------------
-- Testing update_current_terms
---------------------------------------------------------------------------------------------------------

TRUNCATE TABLE banner_terms;
TRUNCATE TABLE current_terms;
TRUNCATE TABLE imports;

-- 1. Testing initial insert.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 1, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_terms (load_id, term_code, term_name_full, term_name, term_start_date, term_end_date, insert_timestamp)
VALUES
	(@LOADID, '120261', 'Spring 2026 - Urbana-Champaign', 'Spring 2026', DATETIME2FROMPARTS(2026, 1, 20, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2026, 5, 14, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120265', 'Summer 2026 - Urbana-Champaign', 'Summer 2026', DATETIME2FROMPARTS(2026, 5, 18, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2026, 8, 8, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120268', 'Fall 2026 - Urbana-Champaign', 'Fall 2026', DATETIME2FROMPARTS(2026, 8, 24, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2026, 12, 17, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120271', 'Spring 2027 - Urbana-Champaign', 'Spring 2027', DATETIME2FROMPARTS(2027, 1, 19, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2027, 5, 13, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120275', 'Summer 2027 - Urbana-Champaign', 'Summer 2027', DATETIME2FROMPARTS(2027, 5, 17, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2027, 8, 7, 0, 0, 0, 0, 1), @DATE)
;

EXEC courseleaf_dev.dbo.update_current_terms;

DROP TABLE IF EXISTS #terms_test_1;
CREATE TABLE #terms_test_1 (
	term_code nvarchar(6),
	term_name_full nvarchar(255),
	term_name nvarchar(255),
	term_start_date datetime2(7),
	term_end_date datetime2(7)
);
INSERT INTO #terms_test_1
	(term_code, term_name_full, term_name, term_start_date, term_end_date)
VALUES
	('120261', 'Spring 2026 - Urbana-Champaign', 'Spring 2026', DATETIME2FROMPARTS(2026, 1, 20, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2026, 5, 14, 0, 0, 0, 0, 1)),
	('120265', 'Summer 2026 - Urbana-Champaign', 'Summer 2026', DATETIME2FROMPARTS(2026, 5, 18, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2026, 8, 8, 0, 0, 0, 0, 1)),
	('120268', 'Fall 2026 - Urbana-Champaign', 'Fall 2026', DATETIME2FROMPARTS(2026, 8, 24, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2026, 12, 17, 0, 0, 0, 0, 1)),
	('120271', 'Spring 2027 - Urbana-Champaign', 'Spring 2027', DATETIME2FROMPARTS(2027, 1, 19, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2027, 5, 13, 0, 0, 0, 0, 1)),
	('120275', 'Summer 2027 - Urbana-Champaign', 'Summer 2027', DATETIME2FROMPARTS(2027, 5, 17, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2027, 8, 7, 0, 0, 0, 0, 1))
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date
FROM
	current_terms
EXCEPT
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date
FROM
	#terms_test_1
)
UNION
(
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date
FROM
	#terms_test_1
EXCEPT
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date
FROM
	current_terms
)
;
GO

-- 2. Testing insert with additional terms.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 2, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_terms (load_id, term_code, term_name_full, term_name, term_start_date, term_end_date, insert_timestamp)
VALUES
	(@LOADID, '120261', 'Spring 2026 - Urbana-Champaign', 'Spring 2026', DATETIME2FROMPARTS(2026, 1, 20, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2026, 5, 14, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120265', 'Summer 2026 - Urbana-Champaign', 'Summer 2026', DATETIME2FROMPARTS(2026, 5, 18, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2026, 8, 8, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120268', 'Fall 2026 - Urbana-Champaign', 'Fall 2026', DATETIME2FROMPARTS(2026, 8, 24, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2026, 12, 17, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120271', 'Spring 2027 - Urbana-Champaign', 'Spring 2027', DATETIME2FROMPARTS(2027, 1, 19, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2027, 5, 13, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120275', 'Summer 2027 - Urbana-Champaign', 'Summer 2027', DATETIME2FROMPARTS(2027, 5, 17, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2027, 8, 7, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120278', 'Fall 2027 - Urbana-Champaign', 'Fall 2027', DATETIME2FROMPARTS(2027, 8, 23, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2027, 12, 16, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120281', 'Spring 2028 - Urbana-Champaign', 'Spring 2028', DATETIME2FROMPARTS(2028, 1, 18, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2028, 5, 11, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120285', 'Summer 2028 - Urbana-Champaign', 'Summer 2028', DATETIME2FROMPARTS(2028, 5, 15, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2028, 8, 5, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120288', 'Fall 2028 - Urbana-Champaign', 'Fall 2028', DATETIME2FROMPARTS(2028, 8, 21, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2028, 12, 14, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120291', 'Spring 2029 - Urbana-Champaign', 'Spring 2029', DATETIME2FROMPARTS(2029, 1, 16, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2029, 5, 10, 0, 0, 0, 0, 1), @DATE)
;

EXEC courseleaf_dev.dbo.update_current_terms;

DROP TABLE IF EXISTS #terms_test_2;
SELECT
	*
INTO
	#terms_test_2
FROM
	#terms_test_1
;
INSERT INTO #terms_test_2
	(term_code, term_name_full, term_name, term_start_date, term_end_date)
VALUES
	('120278', 'Fall 2027 - Urbana-Champaign', 'Fall 2027', DATETIME2FROMPARTS(2027, 8, 23, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2027, 12, 16, 0, 0, 0, 0, 1)),
	('120281', 'Spring 2028 - Urbana-Champaign', 'Spring 2028', DATETIME2FROMPARTS(2028, 1, 18, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2028, 5, 11, 0, 0, 0, 0, 1)),
	('120285', 'Summer 2028 - Urbana-Champaign', 'Summer 2028', DATETIME2FROMPARTS(2028, 5, 15, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2028, 8, 5, 0, 0, 0, 0, 1)),
	('120288', 'Fall 2028 - Urbana-Champaign', 'Fall 2028', DATETIME2FROMPARTS(2028, 8, 21, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2028, 12, 14, 0, 0, 0, 0, 1)),
	('120291', 'Spring 2029 - Urbana-Champaign', 'Spring 2029', DATETIME2FROMPARTS(2029, 1, 16, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2029, 5, 10, 0, 0, 0, 0, 1))

-- Test succeeds if this query returns 0 rows
(
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date
FROM
	current_terms
EXCEPT
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date
FROM
	#terms_test_2
)
UNION
(
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date
FROM
	#terms_test_2
EXCEPT
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date
FROM
	current_terms
)
;
GO

-- 3. Testing insert with term dates changed.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 3, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	banner_terms (load_id, term_code, term_name_full, term_name, term_start_date, term_end_date, insert_timestamp)
VALUES
	(@LOADID, '120261', 'Spring 2026 - Urbana-Champaign', 'Spring 2026', DATETIME2FROMPARTS(2026, 1, 20, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2026, 5, 14, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120265', 'Summer 2026 - Urbana-Champaign', 'Summer 2026', DATETIME2FROMPARTS(2026, 5, 18, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2026, 8, 9, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120268', 'Fall 2026 - Urbana-Champaign', 'Fall 2026', DATETIME2FROMPARTS(2026, 8, 24, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2026, 12, 17, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120271', 'Spring 2027 - Urbana-Champaign', 'Spring 2027', DATETIME2FROMPARTS(2027, 1, 19, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2027, 5, 13, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120275', 'Summer 2027 - Urbana-Champaign', 'Summer 2027', DATETIME2FROMPARTS(2027, 5, 17, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2027, 8, 7, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120278', 'Fall 2027 - Urbana-Champaign', 'Fall 2027', DATETIME2FROMPARTS(2027, 8, 22, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2027, 12, 16, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120281', 'Spring 2028 - Urbana-Champaign', 'Spring 2028', DATETIME2FROMPARTS(2028, 1, 18, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2028, 5, 11, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120285', 'Summer 2028 - Urbana-Champaign', 'Summer 2028', DATETIME2FROMPARTS(2028, 5, 15, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2028, 8, 5, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120288', 'Fall 2028 - Urbana-Champaign', 'Fall 2028', DATETIME2FROMPARTS(2028, 8, 21, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2028, 12, 14, 0, 0, 0, 0, 1), @DATE),
	(@LOADID, '120291', 'Spring 2029 - Urbana-Champaign', 'Spring 2029', DATETIME2FROMPARTS(2029, 1, 17, 0, 0, 0, 0, 1), DATETIME2FROMPARTS(2029, 5, 11, 0, 0, 0, 0, 1), @DATE)
;

EXEC courseleaf_dev.dbo.update_current_terms;

DROP TABLE IF EXISTS #terms_test_3;
SELECT
	*
INTO
	#terms_test_3
FROM
	#terms_test_2
;
UPDATE
	#terms_test_3
SET
	term_start_date = CASE
		WHEN term_code = '120278' THEN DATETIME2FROMPARTS(2027, 8, 22, 0, 0, 0, 0, 1)
		WHEN term_code = '120291' THEN DATETIME2FROMPARTS(2029, 1, 17, 0, 0, 0, 0, 1)
		ELSE term_start_date
	END,
	term_end_date = CASE
		WHEN term_code = '120265' THEN DATETIME2FROMPARTS(2026, 8, 9, 0, 0, 0, 0, 1)
		WHEN term_code = '120291' THEN DATETIME2FROMPARTS(2029, 5, 11, 0, 0, 0, 0, 1)
		ELSE term_end_date
	END
WHERE
	term_code IN ('120265', '120278', '120291')
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date
FROM
	current_terms
EXCEPT
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date
FROM
	#terms_test_3
)
UNION
(
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date
FROM
	#terms_test_3
EXCEPT
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date
FROM
	current_terms
)
;
GO

---------------------------------------------------------------------------------------------------------
-- Testing update_current_users
---------------------------------------------------------------------------------------------------------

-- I used a test data generator to come up with some fun fake names for this one!

TRUNCATE TABLE banner_userinfo;
TRUNCATE TABLE courseleaf_users;
TRUNCATE TABLE current_users;
TRUNCATE TABLE imports;

-- 1. Testing initial insert.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 1, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

-- 1. load user data to courseleaf_users
-- 2. load enhanced data to banner_userinfo
-- 3. execute update_current_users

INSERT INTO
	courseleaf_users (load_id, uin, last_name, first_name, email, insert_timestamp)
VALUES
	(@LOADID, '612154693', 'Scourfield', 'Em', 'jscourfield0@illinois.edu', @DATE),
	(@LOADID, '623106784', 'Blaskett', 'Bertina', 'nblaskett1@illinois.edu', @DATE),
	(@LOADID, '645352686', 'Bayless', 'Feodor', 'jbayless2@illinois.edu', @DATE),
	(@LOADID, '669542329', 'Maruszewski', 'Clo', 'jmaruszewski3@illinois.edu', @DATE),
	(@LOADID, '657459846', 'Trineman', 'Gwendolyn', 'rtrineman4@illinois.edu', @DATE),
	(@LOADID, '658560613', 'Giddins', 'Petr', 'mgiddins5@illinois.edu', @DATE),
	(@LOADID, '673588855', 'Wallis', 'Nolie', 'hwallis6@illinois.edu', @DATE),
	(@LOADID, '620689621', 'Vardy', 'Otis', 'bvardy7@illinois.edu', @DATE),
	(@LOADID, '618121434', 'MacTrustram', 'Carin', 'mmactrustram8@illinois.edu', @DATE),
	(@LOADID, '625710349', 'Spencelayh', 'Stacy', 'mspencelayh9@illinois.edu', @DATE)
;

INSERT INTO
	banner_userinfo (load_id, uin, pidm, first_name, preferred_first_name, middle_name, last_name, name_suffix, email_address, netid, campus_domain, max_activity_date, insert_timestamp)
VALUES
	(@LOADID, '612154693', 1187354, 'Em', 'Eliot', 'Jaimie', 'Scourfield', null, 'jscourfield0@illinois.edu', 'jscourfield0', 'illinois.edu', '2025-09-14 22:56:12', @DATE),
	(@LOADID, '623106784', 11641268, 'Bertina', null, 'Nahum', 'Blaskett', null, 'nblaskett1@illinois.edu', 'nblaskett1', 'illinois.edu', '2026-01-02 12:48:03', @DATE),
	(@LOADID, '645352686', 50749965, 'Feodor', 'Gerhardt', 'Jacklin', 'Bayless', null, 'jbayless2@illinois.edu', 'jbayless2', 'illinois.edu', '2025-06-22 11:49:58', @DATE),
	(@LOADID, '669542329', 30358711, 'Clo', null, 'Joanna', 'Maruszewski', null, 'jmaruszewski3@illinois.edu', 'jmaruszewski3', 'illinois.edu', '2025-09-01 14:57:39', @DATE),
	(@LOADID, '657459846', 78777488, 'Gwendolyn', null, null, 'Trineman', null, 'rtrineman4@illinois.edu', 'btrineman4', 'illinois.edu', '2025-07-16 02:34:34', @DATE),
	(@LOADID, '658560613', 87732278, 'Petr', null, null, 'Giddins', 'Jr.', 'mgiddins5@illinois.edu', 'agiddins5', 'illinois.edu', '2025-09-19 07:09:48', @DATE),
	(@LOADID, '673588855', 24530585, 'Nolie', null, 'Horatia', 'Wallis', null, 'hwallis6@illinois.edu', 'hwallis6', 'illinois.edu', '2025-09-17 12:19:19', @DATE),
	(@LOADID, '620689621', 13415655, 'Otis', null, null, 'Vardy', null, 'bvardy7@illinois.edu', 'cvardy7', 'illinois.edu', '2025-12-29 14:26:02', @DATE),
	(@LOADID, '618121434', 36958360, 'Carin', null, null, 'MacTrustram', null, 'mmactrustram8@illinois.edu', 'gmactrustram8', 'illinois.edu', '2026-05-14 08:11:41', @DATE),
	(@LOADID, '625710349', 77654471, 'Stacy', null, 'Marilyn', 'Spencelayh', null, 'mspencelayh9@illinois.edu', 'mspencelayh9', 'illinois.edu', '2025-08-19 00:36:03', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_users;

DROP TABLE IF EXISTS #users_test_1;
CREATE TABLE #users_test_1 (
	uin nvarchar(9),
	last_name nvarchar(50),
	first_name nvarchar(50),
	email nvarchar(50)
);
INSERT INTO #users_test_1
	(uin, last_name, first_name, email)
VALUES
	('612154693', 'Scourfield', 'Eliot', 'jscourfield0@illinois.edu'),
	('623106784', 'Blaskett', 'Bertina', 'nblaskett1@illinois.edu'),
	('645352686', 'Bayless', 'Gerhardt', 'jbayless2@illinois.edu'),
	('669542329', 'Maruszewski', 'Clo', 'jmaruszewski3@illinois.edu'),
	('657459846', 'Trineman', 'Gwendolyn', 'rtrineman4@illinois.edu'),
	('658560613', 'Giddins Jr.', 'Petr', 'mgiddins5@illinois.edu'),
	('673588855', 'Wallis', 'Nolie', 'hwallis6@illinois.edu'),
	('620689621', 'Vardy', 'Otis', 'bvardy7@illinois.edu'),
	('618121434', 'MacTrustram', 'Carin', 'mmactrustram8@illinois.edu'),
	('625710349', 'Spencelayh', 'Stacy', 'mspencelayh9@illinois.edu')
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	uin, last_name, first_name, email
FROM
	current_users
EXCEPT
SELECT
	uin, last_name, first_name, email
FROM
	#users_test_1
)
UNION
(
SELECT
	uin, last_name, first_name, email
FROM
	#users_test_1
EXCEPT
SELECT
	uin, last_name, first_name, email
FROM
	current_users
)
;
GO

-- 2. Testing re-insert of same data with one new row.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 2, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

INSERT INTO
	courseleaf_users (load_id, uin, last_name, first_name, email, insert_timestamp)
VALUES
	(@LOADID, '612154693', 'Scourfield', 'Em', 'jscourfield0@illinois.edu', @DATE),
	(@LOADID, '623106784', 'Blaskett', 'Bertina', 'nblaskett1@illinois.edu', @DATE),
	(@LOADID, '645352686', 'Bayless', 'Feodor', 'jbayless2@illinois.edu', @DATE),
	(@LOADID, '669542329', 'Maruszewski', 'Clo', 'jmaruszewski3@illinois.edu', @DATE),
	(@LOADID, '657459846', 'Trineman', 'Gwendolyn', 'rtrineman4@illinois.edu', @DATE),
	(@LOADID, '658560613', 'Giddins', 'Petr', 'mgiddins5@illinois.edu', @DATE),
	(@LOADID, '673588855', 'Wallis', 'Nolie', 'hwallis6@illinois.edu', @DATE),
	(@LOADID, '620689621', 'Vardy', 'Otis', 'bvardy7@illinois.edu', @DATE),
	(@LOADID, '618121434', 'MacTrustram', 'Carin', 'mmactrustram8@illinois.edu', @DATE),
	(@LOADID, '625710349', 'Spencelayh', 'Stacy', 'mspencelayh9@illinois.edu', @DATE),
	(@LOADID, '665465435', 'Drusus', 'Kallisto', 'kdrusus@illinois.edu', @DATE)
;

INSERT INTO
	banner_userinfo (load_id, uin, pidm, first_name, preferred_first_name, middle_name, last_name, name_suffix, email_address, netid, campus_domain, max_activity_date, insert_timestamp)
VALUES
	(@LOADID, '612154693', 1187354, 'Em', 'Eliot', 'Jaimie', 'Scourfield', null, 'jscourfield0@illinois.edu', 'jscourfield0', 'illinois.edu', '2025-09-14 22:56:12', @DATE),
	(@LOADID, '623106784', 11641268, 'Bertina', null, 'Nahum', 'Blaskett', null, 'nblaskett1@illinois.edu', 'nblaskett1', 'illinois.edu', '2026-01-02 12:48:03', @DATE),
	(@LOADID, '645352686', 50749965, 'Feodor', 'Gerhardt', 'Jacklin', 'Bayless', null, 'jbayless2@illinois.edu', 'jbayless2', 'illinois.edu', '2025-06-22 11:49:58', @DATE),
	(@LOADID, '669542329', 30358711, 'Clo', null, 'Joanna', 'Maruszewski', null, 'jmaruszewski3@illinois.edu', 'jmaruszewski3', 'illinois.edu', '2025-09-01 14:57:39', @DATE),
	(@LOADID, '657459846', 78777488, 'Gwendolyn', null, null, 'Trineman', null, 'rtrineman4@illinois.edu', 'btrineman4', 'illinois.edu', '2025-07-16 02:34:34', @DATE),
	(@LOADID, '658560613', 87732278, 'Petr', null, null, 'Giddins', 'Jr.', 'mgiddins5@illinois.edu', 'agiddins5', 'illinois.edu', '2025-09-19 07:09:48', @DATE),
	(@LOADID, '673588855', 24530585, 'Nolie', null, 'Horatia', 'Wallis', null, 'hwallis6@illinois.edu', 'hwallis6', 'illinois.edu', '2025-09-17 12:19:19', @DATE),
	(@LOADID, '620689621', 13415655, 'Otis', null, null, 'Vardy', null, 'bvardy7@illinois.edu', 'cvardy7', 'illinois.edu', '2025-12-29 14:26:02', @DATE),
	(@LOADID, '618121434', 36958360, 'Carin', null, null, 'MacTrustram', null, 'mmactrustram8@illinois.edu', 'gmactrustram8', 'illinois.edu', '2026-05-14 08:11:41', @DATE),
	(@LOADID, '625710349', 77654471, 'Stacy', null, 'Marilyn', 'Spencelayh', null, 'mspencelayh9@illinois.edu', 'mspencelayh9', 'illinois.edu', '2025-08-19 00:36:03', @DATE),
	(@LOADID, '665465435', 11223344, 'Kallisto', 'Kalli', 'Carmen', 'Drusus', 'Sr.', 'kdrusus@illinois.edu', 'kdrusus', 'illinois.edu', '2025-08-19 00:36:03', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_users;

DROP TABLE IF EXISTS #users_test_2;
SELECT
	*
INTO
	#users_test_2
FROM
	#users_test_1
;
INSERT INTO #users_test_2
	(uin, last_name, first_name, email)
VALUES
	('665465435', 'Drusus Sr.', 'Kalli', 'kdrusus@illinois.edu')
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	uin, last_name, first_name, email
FROM
	current_users
EXCEPT
SELECT
	uin, last_name, first_name, email
FROM
	#users_test_2
)
UNION
(
SELECT
	uin, last_name, first_name, email
FROM
	#users_test_2
EXCEPT
SELECT
	uin, last_name, first_name, email
FROM
	current_users
)
;
GO

-- 3. Testing insert with data changes.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 3, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

-- I assume that the user data in courseleaf never changes except adding new users/deleting old users.
INSERT INTO
	courseleaf_users (load_id, uin, last_name, first_name, email, insert_timestamp)
VALUES
	(@LOADID, '612154693', 'Scourfield', 'Em', 'jscourfield0@illinois.edu', @DATE),
	(@LOADID, '623106784', 'Blaskett', 'Bertina', 'nblaskett1@illinois.edu', @DATE),
	(@LOADID, '645352686', 'Bayless', 'Feodor', 'jbayless2@illinois.edu', @DATE),
	(@LOADID, '669542329', 'Maruszewski', 'Clo', 'jmaruszewski3@illinois.edu', @DATE),
	(@LOADID, '657459846', 'Trineman', 'Gwendolyn', 'rtrineman4@illinois.edu', @DATE),
	(@LOADID, '658560613', 'Giddins', 'Petr', 'mgiddins5@illinois.edu', @DATE),
	(@LOADID, '673588855', 'Wallis', 'Nolie', 'hwallis6@illinois.edu', @DATE),
	(@LOADID, '620689621', 'Vardy', 'Otis', 'bvardy7@illinois.edu', @DATE),
	(@LOADID, '618121434', 'MacTrustram', 'Carin', 'mmactrustram8@illinois.edu', @DATE),
	(@LOADID, '625710349', 'Spencelayh', 'Stacy', 'mspencelayh9@illinois.edu', @DATE),
	(@LOADID, '665465435', 'Drusus', 'Kallisto', 'kdrusus@illinois.edu', @DATE)
;

-- Some preferred names have been changed as well as some emails.
INSERT INTO
	banner_userinfo (load_id, uin, pidm, first_name, preferred_first_name, middle_name, last_name, name_suffix, email_address, netid, campus_domain, max_activity_date, insert_timestamp)
VALUES
	(@LOADID, '612154693', 1187354, 'Em', 'Eliot', 'Jaimie', 'Scourfield', null, 'jscourfield0@illinois.edu', 'jscourfield0', 'illinois.edu', '2025-09-14 22:56:12', @DATE),
	(@LOADID, '623106784', 11641268, 'Bertina', null, 'Nahum', 'Blaskett', null, 'nblaskett1@illinois.edu', 'nblaskett1', 'illinois.edu', '2026-01-02 12:48:03', @DATE),
	(@LOADID, '645352686', 50749965, 'Feodor', 'Gerhardt', 'Jacklin', 'Bayless', null, 'gerhardt@illinois.edu', 'jbayless2', 'illinois.edu', '2025-06-22 11:49:58', @DATE),
	(@LOADID, '669542329', 30358711, 'Clo', null, 'Joanna', 'Maruszewski', null, 'jmaruszewski3@illinois.edu', 'jmaruszewski3', 'illinois.edu', '2025-09-01 14:57:39', @DATE),
	(@LOADID, '657459846', 78777488, 'Gwendolyn', 'Gary', null, 'Trineman', null, 'rtrineman4@illinois.edu', 'btrineman4', 'illinois.edu', '2025-07-16 02:34:34', @DATE),
	(@LOADID, '658560613', 87732278, 'Petr', null, null, 'Giddins', 'Jr.', 'mgiddins5@illinois.edu', 'agiddins5', 'illinois.edu', '2025-09-19 07:09:48', @DATE),
	(@LOADID, '673588855', 24530585, 'Nolie', null, 'Horatia', 'Wallis', null, 'hwallis6@illinois.edu', 'hwallis6', 'illinois.edu', '2025-09-17 12:19:19', @DATE),
	(@LOADID, '620689621', 13415655, 'Otis', null, null, 'Vardy', null, 'bvardy7@illinois.edu', 'cvardy7', 'illinois.edu', '2025-12-29 14:26:02', @DATE),
	(@LOADID, '618121434', 36958360, 'Carin', null, null, 'MacTrustram', null, 'mmactrustram8@illinois.edu', 'gmactrustram8', 'illinois.edu', '2026-05-14 08:11:41', @DATE),
	(@LOADID, '625710349', 77654471, 'Stacy', 'Steven', 'Marilyn', 'Spencelayh', null, 'mspencelayh9@illinois.edu', 'mspencelayh9', 'illinois.edu', '2025-08-19 00:36:03', @DATE),
	(@LOADID, '665465435', 11223344, 'Kallisto', 'Kalli', 'Carmen', 'Drusus', 'Sr.', 'kdrusus@illinois.edu', 'kdrusus', 'illinois.edu', '2025-08-19 00:36:03', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_users;

DROP TABLE IF EXISTS #users_test_3;
SELECT
	*
INTO
	#users_test_3
FROM
	#users_test_2
;
UPDATE
	#users_test_3
SET
	first_name = 'Steven'
WHERE
	uin = '625710349'
;
UPDATE
	#users_test_3
SET
	email = 'gerhardt@illinois.edu'
WHERE
	uin = '645352686'
;
UPDATE
	#users_test_3
SET
	first_name = 'Gary'
WHERE
	uin = '657459846'
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	uin, last_name, first_name, email
FROM
	current_users
EXCEPT
SELECT
	uin, last_name, first_name, email
FROM
	#users_test_3
)
UNION
(
SELECT
	uin, last_name, first_name, email
FROM
	#users_test_3
EXCEPT
SELECT
	uin, last_name, first_name, email
FROM
	current_users
)
;
GO

-- 4. Testing user removal.

DECLARE @DATE datetime2(7);
SET @DATE = DATETIME2FROMPARTS(2026, 1, 4, 6, 0, 0, 0, 1);

INSERT INTO
	imports (load_begin_timestamp)
VALUES
	(@DATE)
;

DECLARE @LOADID int;
SET @LOADID = courseleaf_dev.dbo.get_max_load_id();

-- I assume that the user data in courseleaf never changes except adding new users/deleting old users.
INSERT INTO
	courseleaf_users (load_id, uin, last_name, first_name, email, insert_timestamp)
VALUES
	(@LOADID, '612154693', 'Scourfield', 'Em', 'jscourfield0@illinois.edu', @DATE),
	(@LOADID, '623106784', 'Blaskett', 'Bertina', 'nblaskett1@illinois.edu', @DATE),
	(@LOADID, '645352686', 'Bayless', 'Feodor', 'jbayless2@illinois.edu', @DATE),
	(@LOADID, '669542329', 'Maruszewski', 'Clo', 'jmaruszewski3@illinois.edu', @DATE),
	(@LOADID, '657459846', 'Trineman', 'Gwendolyn', 'rtrineman4@illinois.edu', @DATE),
	(@LOADID, '658560613', 'Giddins', 'Petr', 'mgiddins5@illinois.edu', @DATE),
	(@LOADID, '673588855', 'Wallis', 'Nolie', 'hwallis6@illinois.edu', @DATE),
	(@LOADID, '620689621', 'Vardy', 'Otis', 'bvardy7@illinois.edu', @DATE),
	(@LOADID, '618121434', 'MacTrustram', 'Carin', 'mmactrustram8@illinois.edu', @DATE),
	(@LOADID, '625710349', 'Spencelayh', 'Stacy', 'mspencelayh9@illinois.edu', @DATE)
;

-- Some preferred names have been changed as well as some emails.
INSERT INTO
	banner_userinfo (load_id, uin, pidm, first_name, preferred_first_name, middle_name, last_name, name_suffix, email_address, netid, campus_domain, max_activity_date, insert_timestamp)
VALUES
	(@LOADID, '612154693', 1187354, 'Em', 'Eliot', 'Jaimie', 'Scourfield', null, 'jscourfield0@illinois.edu', 'jscourfield0', 'illinois.edu', '2025-09-14 22:56:12', @DATE),
	(@LOADID, '623106784', 11641268, 'Bertina', null, 'Nahum', 'Blaskett', null, 'nblaskett1@illinois.edu', 'nblaskett1', 'illinois.edu', '2026-01-02 12:48:03', @DATE),
	(@LOADID, '645352686', 50749965, 'Feodor', 'Gerhardt', 'Jacklin', 'Bayless', null, 'gerhardt@illinois.edu', 'jbayless2', 'illinois.edu', '2025-06-22 11:49:58', @DATE),
	(@LOADID, '669542329', 30358711, 'Clo', null, 'Joanna', 'Maruszewski', null, 'jmaruszewski3@illinois.edu', 'jmaruszewski3', 'illinois.edu', '2025-09-01 14:57:39', @DATE),
	(@LOADID, '657459846', 78777488, 'Gwendolyn', 'Gary', null, 'Trineman', null, 'rtrineman4@illinois.edu', 'btrineman4', 'illinois.edu', '2025-07-16 02:34:34', @DATE),
	(@LOADID, '658560613', 87732278, 'Petr', null, null, 'Giddins', 'Jr.', 'mgiddins5@illinois.edu', 'agiddins5', 'illinois.edu', '2025-09-19 07:09:48', @DATE),
	(@LOADID, '673588855', 24530585, 'Nolie', null, 'Horatia', 'Wallis', null, 'hwallis6@illinois.edu', 'hwallis6', 'illinois.edu', '2025-09-17 12:19:19', @DATE),
	(@LOADID, '620689621', 13415655, 'Otis', null, null, 'Vardy', null, 'bvardy7@illinois.edu', 'cvardy7', 'illinois.edu', '2025-12-29 14:26:02', @DATE),
	(@LOADID, '618121434', 36958360, 'Carin', null, null, 'MacTrustram', null, 'mmactrustram8@illinois.edu', 'gmactrustram8', 'illinois.edu', '2026-05-14 08:11:41', @DATE),
	(@LOADID, '625710349', 77654471, 'Stacy', 'Steven', 'Marilyn', 'Spencelayh', null, 'mspencelayh9@illinois.edu', 'mspencelayh9', 'illinois.edu', '2025-08-19 00:36:03', @DATE)
;

EXEC courseleaf_dev.dbo.update_current_users;

DROP TABLE IF EXISTS #users_test_4;
SELECT
	*
INTO
	#users_test_4
FROM
	#users_test_3
;
DELETE FROM
	#users_test_4
WHERE
	uin = '665465435'
;

-- Test succeeds if this query returns 0 rows
(
SELECT
	uin, last_name, first_name, email
FROM
	current_users
EXCEPT
SELECT
	uin, last_name, first_name, email
FROM
	#users_test_4
)
UNION
(
SELECT
	uin, last_name, first_name, email
FROM
	#users_test_4
EXCEPT
SELECT
	uin, last_name, first_name, email
FROM
	current_users
)
;
GO
