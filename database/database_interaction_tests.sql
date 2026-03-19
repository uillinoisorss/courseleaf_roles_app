---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
-- Test code for CourseLeafApp database trigger/SP interactions
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------
-- Tests for update_current_roles and deactivate_old_roles
---------------------------------------------------------------------------------------------------------
TRUNCATE TABLE [CourseLeaf_Contacts].[dbo].[courseleaf_roles];
TRUNCATE TABLE [CourseLeaf_Contacts].[dbo].[current_roles];

-- Inserts to courseleaf_roles trigger update_current_roles
INSERT INTO
	[CourseLeaf_Contacts].[dbo].[courseleaf_roles] (load_id, role, dept_no, dept, role_title, uin, insert_timestamp)
VALUES 
	(1, '1211-CIC Head', '1211', 'CIC', 'Head', '675086358', GETDATE()),
	(1, '1211-CIC Head', '1211', 'CIC', 'Head', '658725029', GETDATE()),
	(1, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656120313', GETDATE()),
	(1, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656058033', GETDATE()),
	(1, '1248-ES Head', '1248', 'ES', 'Head', '657954214', GETDATE()),
	(1, '1248-ES Head', '1248', 'ES', 'Head', '656058033', GETDATE())
;

INSERT INTO
	[CourseLeaf_Contacts].[dbo].[courseleaf_roles] (load_id, role, dept_no, dept, role_title, uin, insert_timestamp)
VALUES
	(2, '1211-CIC Head', '1211', 'CIC', 'Head', '675086358', GETDATE()),
	(2, '1211-CIC Head', '1211', 'CIC', 'Head', '656120313', GETDATE()),
	(2, '1211-CIC Head', '1211', 'CIC', 'Head', '658725029', GETDATE()),
	(2, '1241-ANTH Head', '1241', 'ANTH', 'Head', '656120313', GETDATE()),
	(2, '1248-ES Head', '1248', 'ES', 'Head', '657954214', GETDATE())
;

-- This SP will need to be called after all inserts (to courseleaf_roles) are done
EXEC [CourseLeaf_Contacts].[dbo].[deactivate_old_roles];

-- Manually check results (can make automated tests later)
SELECT * FROM [CourseLeaf_Contacts].[dbo].[courseleaf_roles];
SELECT * FROM [CourseLeaf_Contacts].[dbo].[current_roles];

---------------------------------------------------------------------------------------------------------
-- Tests for update_current_courses and update_current_crosslists
---------------------------------------------------------------------------------------------------------
TRUNCATE TABLE [CourseLeaf_Contacts].[dbo].[banner_courses];
TRUNCATE TABLE [CourseLeaf_Contacts].[dbo].[current_courses];

INSERT INTO
	[CourseLeaf_Contacts].[dbo].[banner_courses] (load_id, term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status, insert_timestamp)
VALUES
	('1', '120261', 'AE 199', 'AE', '199', 'Undergraduate Open Seminar', 'KP', '1615', NULL, '1000001', '120048', '999999', '120228', 'A', GETDATE()),
	('1', '120261', 'ACCY 410', 'ACCY', '410', 'Advanced Financial Reporting', 'KM', '1346', NULL, '1000079', '120048', '999999', '120228', 'A', GETDATE()),
	('1', '120261', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A', GETDATE()),
	('1', '120261', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', GETDATE()),
	('1', '120261', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', GETDATE()),
	('1', '120261', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', GETDATE()),
	('1', '120261', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', GETDATE())
;

EXEC [CourseLeaf_Contacts].[dbo].[update_current_crosslists];

INSERT INTO
	[CourseLeaf_Contacts].[dbo].[banner_courses] (load_id, term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status, insert_timestamp)
VALUES
	('2', '120261', 'ACE 210', 'ACE', '210', 'Environmental Economics & Policy', 'KL', '1470', 'C', '1000142', '120048', '999999', '120258', 'A', GETDATE()),
	('2', '120261', 'ENVS 210', 'ENVS', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', GETDATE()),
	('2', '120261', 'NRES 210', 'NRES', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', GETDATE()),
	('2', '120261', 'UP 210', 'UP', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', GETDATE()),
	('2', '120261', 'ECON 210', 'ECON', '210', 'Environmental Economics & Policy', 'KL', '1470', 'N', '1000142', '120048', '999999', '120258', 'A', GETDATE())
;

EXEC [CourseLeaf_Contacts].[dbo].[update_current_crosslists];

SELECT * FROM [CourseLeaf_Contacts].[dbo].[banner_courses];
SELECT * FROM [CourseLeaf_Contacts].[dbo].[current_courses];
SELECT * FROM [CourseLeaf_Contacts].[dbo].[current_crosslists] ORDER BY [controlling_course];

---------------------------------------------------------------------------------------------------------
-- Tests for update_current_departments
---------------------------------------------------------------------------------------------------------

TRUNCATE TABLE [CourseLeaf_Contacts].[dbo].[courseleaf_departments];
TRUNCATE TABLE [CourseLeaf_Contacts].[dbo].[current_departments];

INSERT INTO
	[CourseLeaf_Contacts].[dbo].[courseleaf_departments] (load_id, dept_no, dept_name, college, college_name, insert_timestamp)
VALUES
	('1', '1211', 'CIC Traveling Scholars', NULL, NULL, GETDATE()),
	('1', '1230', 'MBA Program Administration', NULL, NULL, GETDATE()),
	('1', '1241', 'Anthropology', 'KV', 'Liberal Arts & Sciences', GETDATE()),
	('1', '1244', 'Physics', 'KP', 'Grainger College of Engineering', GETDATE()),
	('1', '1246', 'Computational Science & Engr', 'KP', 'Grainger College of Engineering', GETDATE())
;

SELECT * FROM [CourseLeaf_Contacts].[dbo].[courseleaf_departments];
SELECT * FROM [CourseLeaf_Contacts].[dbo].[current_departments];

---------------------------------------------------------------------------------------------------------
-- Tests for update_current_subjects
---------------------------------------------------------------------------------------------------------

TRUNCATE TABLE [CourseLeaf_Contacts].[dbo].[courseleaf_subjects];
TRUNCATE TABLE [CourseLeaf_Contacts].[dbo].[current_subjects];

INSERT INTO
	[CourseLeaf_Contacts].[dbo].[courseleaf_subjects] (load_id, subject_code, subject, dept_no, insert_timestamp)
VALUES
	('1', 'AAS', 'Asian American Studies', '1404', GETDATE()),
	('1', 'ABE', 'Agric & Biological Engineering', '1741', GETDATE()),
	('1', 'ACCY', 'Accountancy', '1346', GETDATE()),
	('1', 'ACE', 'Agr & Consumer Economics', '1470', GETDATE()),
	('1', 'ACES', 'Agr, Consumer, & Env Sciences', '1306', GETDATE())
;

SELECT * FROM [CourseLeaf_Contacts].[dbo].[courseleaf_subjects];
SELECT * FROM [CourseLeaf_Contacts].[dbo].[current_subjects];

---------------------------------------------------------------------------------------------------------
-- Tests for update_current_users
---------------------------------------------------------------------------------------------------------

TRUNCATE TABLE [CourseLeaf_Contacts].[dbo].[courseleaf_users];
TRUNCATE TABLE [CourseLeaf_Contacts].[dbo].[current_users];

INSERT INTO
	[CourseLeaf_Contacts].[dbo].[courseleaf_users] (load_id, uin, last_name, first_name, email, insert_timestamp)
VALUES
	('1', '669756033', 'Abbott', 'Matt', 'abbtt@illinois.edu', GETDATE()),
	('1', '658218209', 'Abd-El-Khalick', 'Fouad', 'fouad@illinois.edu', GETDATE()),
	('1', '658821131', 'Admin', 'Leepfrog', 'clhelp@courseleaf.com', GETDATE()),
	('1', '652810923', 'Alexander', 'Craig', 'cmalexan@illinois.edu', GETDATE()),
	('1', '679531621', 'Allan', 'Brian', 'ballan@illinois.edu', GETDATE())
;

SELECT * FROM [CourseLeaf_Contacts].[dbo].[courseleaf_users];
SELECT * FROM [CourseLeaf_Contacts].[dbo].[current_users];