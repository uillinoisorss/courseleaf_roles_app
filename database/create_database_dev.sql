-- Run this code to create the CourseLeaf application database from scratch.
-- This will delete and recreate everything in the process, so only run if
-- you aren't worried about losing any data.

USE [courseleaf_dev];
GO

---------------------------------------------------------------------------------------------------
-- TABLES
---------------------------------------------------------------------------------------------------

-- Create imports table

DROP TABLE IF EXISTS [imports];
GO

CREATE TABLE [imports] (
	[load_id] int PRIMARY KEY IDENTITY(1, 1),
	[load_begin_timestamp] datetime2(7),
	[load_end_timestamp] datetime2(7),
	[banner_courses_rows_inserted] int,
	[banner_departments_rows_inserted] int,
	[banner_subjects_rows_inserted] int,
	[banner_terms_rows_inserted] int,
	[banner_userinfo_rows_inserted] int,
	[courseleaf_roles_rows_inserted] int,
	[courseleaf_users_rows_inserted] int
)
GO

-- Create Banner tables

DROP TABLE IF EXISTS [banner_courses];
GO

CREATE TABLE [banner_courses] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [load_id] int,
  [term] nvarchar(6),
  [course] nvarchar(50),
  [subject_code] nvarchar(50),
  [course_no] nvarchar(3),
  [course_title] nvarchar(255),
  [college] nvarchar(2),
  [dept_no] nvarchar(4),
  [control_code] nvarchar(1),
  [course_id] nvarchar(7),
  [course_start_term] nvarchar(6),
  [course_end_term] nvarchar(6),
  [course_effective_term] nvarchar(6),
  [status] nvarchar(1),
  [insert_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [banner_departments];
GO

CREATE TABLE [banner_departments] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [load_id] int,
  [dept_no] nvarchar(4),
  [dept_name] nvarchar(255),
  [insert_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [banner_subjects];
GO

CREATE TABLE [banner_subjects] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [load_id] int,
  [subject_code] nvarchar(50),
  [subject] nvarchar(50),
  [subject_name_codebook] nvarchar(255),
  [insert_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [banner_terms];
GO

CREATE TABLE [banner_terms] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [load_id] int,
  [term_code] nvarchar(6),
  [term_name_full] nvarchar(255),
  [term_name] nvarchar(50),
  [term_start_date] datetime2(7),
  [term_end_date] datetime2(7),
  [insert_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [banner_userinfo];
GO

CREATE TABLE [banner_userinfo] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [load_id] int,
  [uin] nvarchar(9),
  [pidm] int,
  [first_name] nvarchar(50),
  [preferred_first_name] nvarchar(50),
  [middle_name] nvarchar(50),
  [last_name] nvarchar(50),
  [name_suffix] nvarchar(50),
  [email_address] nvarchar(50),
  [netid] nvarchar(50),
  [campus_domain] nvarchar(50),
  [max_activity_date] datetime2(7),
  [insert_timestamp] datetime2(7)
)
GO

-- Create CourseLeaf tables

DROP TABLE IF EXISTS [courseleaf_roles];
GO

CREATE TABLE [courseleaf_roles] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [load_id] int,
  [role] nvarchar(50),
  [dept_no] nvarchar(4),
  [dept] nvarchar(50),
  [role_title] nvarchar(50),
  [uin] nvarchar(9),
  [insert_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [courseleaf_users];
GO

CREATE TABLE [courseleaf_users] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [load_id] int,
  [uin] nvarchar(9),
  [last_name] nvarchar(50),
  [first_name] nvarchar(50),
  [email] nvarchar(50),
  [insert_timestamp] datetime2(7)
)
GO

-- Create Current tables

DROP TABLE IF EXISTS [current_courses];
GO

CREATE TABLE [current_courses] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [term] nvarchar(6),
  [course] nvarchar(50),
  [subject_code] nvarchar(50),
  [course_no] nvarchar(3),
  [course_title] nvarchar(255),
  [college] nvarchar(2),
  [dept_no] nvarchar(4),
  [control_code] nvarchar(1),
  [course_id] nvarchar(7),
  [course_start_term] nvarchar(6),
  [course_end_term] nvarchar(6),
  [course_effective_term] nvarchar(6),
  [status] nvarchar(1),
  [insert_timestamp] datetime2(7),
  [modified_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [current_crosslists];
GO

CREATE TABLE [current_crosslists] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [crosslist_effective_term] nvarchar(6),
  [course] nvarchar(50),
  [controlling_course] nvarchar(50),
  [controlling_dept_no] nvarchar(4),
  [is_crosslisted] nvarchar(1),
  [is_controlling] nvarchar(1),
  [insert_timestamp] datetime2(7),
  [modified_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [current_departments];
GO

CREATE TABLE [current_departments] (
  [dept_no] nvarchar(4) PRIMARY KEY,
  [dept_name] nvarchar(255),
  [insert_timestamp] datetime2(7),
  [modified_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [current_roles];
GO

CREATE TABLE [current_roles] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [uin] nvarchar(9),
  [role] nvarchar(50),
  [sequence_number] int,
  [dept_no] nvarchar(4),
  [dept] nvarchar(50),
  [role_title] nvarchar(50),
  [role_begin_date] date,
  [role_end_date] date,
  [insert_timestamp] datetime2(7),
  [modified_timestamp] datetime2(7)
)
GO

-- Unsure whether index is necessary

--CREATE UNIQUE INDEX current_roles_index
--ON current_roles (uin, role, sequence_number);
--GO

DROP TABLE IF EXISTS [current_subjects];
GO

CREATE TABLE [current_subjects] (
  [subject_code] nvarchar(50) PRIMARY KEY,
  [subject] nvarchar(50),
  [subject_name_codebook] nvarchar(255),
  [insert_timestamp] datetime2(7),
  [modified_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [current_terms];
GO

CREATE TABLE [current_terms] (
  [term_code] nvarchar(6) PRIMARY KEY,
  [term_name_full] nvarchar(255),
  [term_name] nvarchar(255),
  [term_start_date] datetime2(7),
  [term_end_date] datetime2(7),
  [insert_timestamp] datetime2(7),
  [modified_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [current_users];
GO

CREATE TABLE [current_users] (
  [uin] nvarchar(9) PRIMARY KEY,
  [last_name] nvarchar(50),
  [first_name] nvarchar(50),
  [email] nvarchar(50),
  [insert_timestamp] datetime2(7),
  [modified_timestamp] datetime2(7)
)
GO

-- Create OR Tables

DROP TABLE IF EXISTS [ormaintenance_terms];
GO

CREATE TABLE [ormaintenance_terms] (
  [current_term] nvarchar(6) PRIMARY KEY,
  [previous_term] nvarchar(6),
  [next_term] nvarchar(6),
  [next_next_term] nvarchar(6),
  [insert_timestamp] datetime2(7)
)
GO

-- Create PowerBI reference tables

DROP TABLE IF EXISTS [powerbi_crosslists];
GO

CREATE TABLE [powerbi_crosslists] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [crosslist_effective_term] nvarchar(6),
  [selected_course] nvarchar(50),
  [controlling_course] nvarchar(50),
  [courses_in_group] nvarchar(50),
  [is_controlling] nvarchar(1),
  [is_crosslisted] nvarchar(1)
)
GO

---------------------------------------------------------------------------------------------------
-- FUNCTIONS
---------------------------------------------------------------------------------------------------


CREATE FUNCTION get_max_load_id ()
RETURNS INT
AS
BEGIN

	DECLARE @LOADID INT;

	SET @LOADID = (
		SELECT
			MAX(load_id)
		FROM
			imports
	);

	RETURN @LOADID

END
GO


---------------------------------------------------------------------------------------------------
-- STORED PROCEDURES
---------------------------------------------------------------------------------------------------

-- Produces a table of crosslist data specially formatted to appease the Power BI data model.
CREATE OR ALTER PROCEDURE generate_powerbi_crosslists
AS
BEGIN
	SET NOCOUNT ON;

    TRUNCATE TABLE powerbi_crosslists;

	WITH crosslists AS
	(
	SELECT
		crosslist_effective_term,
		course,
		controlling_course,
		is_crosslisted,
		is_controlling
	FROM
		current_crosslists
	),
	is_not_self AS
	(
	SELECT
		crosslists.crosslist_effective_term as term,
		crosslists.course AS selected_course,
		crosslists.controlling_course AS controlling_course,
		courses.course AS courses_in_group,
		courses.is_crosslisted AS is_crosslisted,
		courses.is_controlling AS is_controlling
	FROM
		crosslists
		JOIN crosslists courses ON (
			crosslists.course <> courses.course
			AND crosslists.controlling_course = courses.controlling_course
			AND crosslists.crosslist_effective_term = courses.crosslist_effective_term
		)
	WHERE
		crosslists.controlling_course IS NOT NULL
	),
	is_self AS
	(
	SELECT
		crosslists.crosslist_effective_term as term,
		crosslists.course AS selected_course,
		crosslists.controlling_course AS controlling_course,
		courses.course AS courses_in_group,
		courses.is_crosslisted AS is_crosslisted,
		courses.is_controlling AS is_controlling
	FROM
		crosslists
		JOIN crosslists courses ON (
			crosslists.course = courses.course
			AND crosslists.controlling_course = courses.controlling_course
			AND crosslists.crosslist_effective_term = courses.crosslist_effective_term
		)
	WHERE
		crosslists.controlling_course IS NOT NULL
	),
	all_courses AS
	(
	SELECT
		*
	FROM
		is_not_self
	UNION
	SELECT
		*
	FROM
		is_self
	)
	INSERT INTO
		dbo.powerbi_crosslists (crosslist_effective_term, selected_course, controlling_course, courses_in_group, is_crosslisted, is_controlling)
	SELECT
		all_courses.*
	FROM
		all_courses
	ORDER BY
		controlling_course,
		selected_course,
		term,
		courses_in_group
	;
END
GO

-- Calculate rows inserted from most recent import and record those counts in the imports table.
CREATE OR ALTER PROCEDURE get_import_rowcounts
AS
BEGIN
	SET NOCOUNT ON

	DECLARE @LOADID INT;
	SET @LOADID = (SELECT dbo.get_max_load_id());

	UPDATE
		imports
	SET
		load_end_timestamp = GETDATE(),
		banner_courses_rows_inserted = (SELECT COUNT(*) FROM banner_courses WHERE load_id = @LOADID),
		banner_departments_rows_inserted = (SELECT COUNT(*) FROM banner_departments WHERE load_id = @LOADID),
		banner_subjects_rows_inserted = (SELECT COUNT(*) FROM banner_subjects WHERE load_id = @LOADID),
		banner_terms_rows_inserted = (SELECT COUNT(*) FROM banner_terms WHERE load_id = @LOADID),
		banner_userinfo_rows_inserted = (SELECT COUNT(*) FROM banner_userinfo WHERE load_id = @LOADID),
		courseleaf_roles_rows_inserted = (SELECT COUNT(*) FROM courseleaf_roles WHERE load_id = @LOADID),
		courseleaf_users_rows_inserted = (SELECT COUNT(*) FROM courseleaf_users WHERE load_id = @LOADID)
	WHERE
		load_id = @LOADID
	;
END
GO

-- Update the current_courses table with information from most recent Banner import.
CREATE OR ALTER PROCEDURE update_current_courses
AS
BEGIN
	SET NOCOUNT ON

	-- Current date to be used for timestamps.
	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	-- Load id of most recent import to only get the newest data.
	DECLARE @LOADID INT;
	SET @LOADID = (SELECT dbo.get_max_load_id());

	-- Insert new courses that don't have a matching record in the current_courses table.
	INSERT INTO current_courses (
		term,
		course,
		subject_code,
		course_no,
		course_title,
		college,
		dept_no,
		control_code,
		course_id,
		course_start_term,
		course_end_term,
		course_effective_term,
		status,
		insert_timestamp,
		modified_timestamp
	)
	SELECT
		term,
		course,
		subject_code,
		course_no,
		course_title,
		college,
		dept_no,
		control_code,
		course_id,
		course_start_term,
		course_end_term,
		course_effective_term,
		status,
		@DATE,
		NULL
	FROM
		banner_courses
	WHERE
		banner_courses.load_id = @LOADID
		AND NOT EXISTS (
			SELECT
				current_courses.course_id
			FROM
				current_courses
			WHERE
				current_courses.term = banner_courses.term
				AND current_courses.course = banner_courses.course
				AND current_courses.course_id = banner_courses.course_id
		)
	;

	-- If a course record would be inserted that has the same ID as a course that already exists
	-- in the current_courses table, then we need to check whether the new record has an updated
	-- course_effective_term. If it does, we just modify the existing record with the new
	-- term information.
	UPDATE
		current_courses
	SET
		current_courses.course_start_term = banner_courses.course_start_term,
		current_courses.course_end_term = banner_courses.course_end_term,
		current_courses.course_effective_term = banner_courses.course_effective_term,
		current_courses.modified_timestamp = @DATE
	FROM
		banner_courses
	WHERE
		banner_courses.load_id = @LOADID
		AND current_courses.term = banner_courses.term
		AND current_courses.course = banner_courses.course
		AND current_courses.course_id = banner_courses.course_id
		AND current_courses.course_effective_term < banner_courses.course_effective_term
	;

	-- Don't need to worry about ever marking courses as "deactivated" since we can
	-- always refer to the course start and end terms in Banner.
END
GO

-- Populates crosslist correspondence table: 'course' is the course that gets looked up,
-- 'controlling_course' is the course that controls the course that gets looked up.
CREATE OR ALTER PROCEDURE update_current_crosslists
AS
BEGIN
	SET NOCOUNT ON

	-- This is one instance where I think that truncating the table and re-creating the 
	-- current data relationships makes sense. I do NOT want to track historical
	-- crosslisting data because that would very rapidly become a mess.
	-- I had a brief panic about whether this would actually work, but then it occurred to
	-- me that since the courses from past terms in the current_courses table stop getting
	-- updated when that term is no longer in the window of relevant terms, the crosslist 
	-- data will always recreate the same. Maybe not the most efficient approach to this
	-- issue but should work for now. Hurray!
	TRUNCATE TABLE current_crosslists;
	
	-- Instead of just truncating, maybe only delete the crosslists from the currently active
	-- terms? This means that old crosslists aren't getting deleted and recreated every day.
	-- Will need to update the insert logic below to only insert crosslists that are valid
	-- for one of the active terms.

	--DELETE
	--FROM
	--	current_crosslists
	--WHERE
	--	current_crosslists.crosslist_effective_term IN (
	--		(SELECT current_term FROM ormaintenance_terms),
	--		(SELECT previous_term FROM ormaintenance_terms),
	--		(SELECT next_term FROM ormaintenance_terms),
	--		(SELECT next_next_term FROM ormaintenance_terms)
	--	)
	--;

	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	INSERT INTO current_crosslists (
		crosslist_effective_term,
		course,
		controlling_course,
		controlling_dept_no,
		is_crosslisted,
		is_controlling,
		insert_timestamp
	)
	-- Crosslisted courses that are non-controlling get listed alongside their controlling course here
	SELECT
		non_controlling.term AS crosslist_effective_term,
		non_controlling.course AS course,
		controlling.course AS controlling_course,
		controlling.dept_no AS controlling_dept_no,
		'Y' AS is_crosslisted,
		'N' AS is_controlling,
		@DATE AS insert_timestamp
	FROM
		current_courses non_controlling
		LEFT OUTER JOIN current_courses controlling on (
			non_controlling.course_id = controlling.course_id
			AND non_controlling.term = controlling.term
		)
	WHERE
		non_controlling.control_code = 'N'
		AND controlling.control_code = 'C'
	UNION
	-- Crosslisted courses that are controlling get listed as their own controlling course
	SELECT
		controlling.term AS crosslist_effective_term,
		controlling.course AS course,
		controlling.course AS controlling_course,
		controlling.dept_no AS controlling_dept_no,
		'Y' AS is_crosslisted,
		'Y' AS is_controlling,
		@DATE AS insert_timestamp
	FROM
		current_courses controlling
	WHERE
		controlling.control_code = 'C'
	UNION
	 -- Courses that are not crosslisted also get listed as their own controlling course
	SELECT
		current_courses.term AS crosslist_effective_term,
		current_courses.course AS course,
		current_courses.course AS controlling_course,
		current_courses.dept_no AS controlling_dept_no,
		'N' AS is_crosslisted,
		'Y' AS is_controlling,
		@DATE AS insert_timestamp
	FROM
		current_courses
	WHERE
		current_courses.control_code = ''
		OR current_courses.control_code IS NULL
	;
END
GO

-- Update the current_departments table with information from most recent Banner import.
CREATE OR ALTER PROCEDURE update_current_departments
AS
BEGIN
	SET NOCOUNT ON

	-- Current date to be used for timestamps.
	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	-- Load id of most recent import to only get the newest data.
	DECLARE @LOADID INT;
	SET @LOADID = (SELECT dbo.get_max_load_id());

	-- Look for new department number records that need to be inserted.
	INSERT INTO
		current_departments (
			dept_no,
			dept_name,
			insert_timestamp
		)
	SELECT
		banner_departments.dept_no,
		banner_departments.dept_name,
		@DATE
	FROM
		banner_departments
	WHERE
		banner_departments.load_id = @LOADID
		AND NOT EXISTS (
			SELECT
				current_departments.dept_name,
				current_departments.dept_no
			FROM
				current_departments
			WHERE
				current_departments.dept_no = banner_departments.dept_no
		)
	;

	-- Check whether any existing departments need to have their name updated.
	UPDATE
		current_departments
	SET
		current_departments.dept_name = banner_departments.dept_name,
		current_departments.modified_timestamp = @DATE
	FROM
		banner_departments
	WHERE
		banner_departments.load_id = @LOADID
		AND current_departments.dept_no = banner_departments.dept_no
		AND current_departments.dept_name <> banner_departments.dept_name
	;

	-- Identify any department numbers that were not present in the most recent
	-- Banner import.
	SELECT
		current_departments.dept_no
	INTO
		#old_departments
	FROM
		current_departments
	WHERE
		NOT EXISTS (
			SELECT
				*
			FROM
				banner_departments
			WHERE
				banner_departments.dept_no = current_departments.dept_no
				AND banner_departments.load_id = @LOADID
		)
	;

	-- Delete departments that were not present in most recent Banner import.
	DELETE
	FROM
		current_departments
	WHERE
		current_departments.dept_no IN (SELECT dept_no FROM #old_departments)
	;
END
GO

-- Update the current_roles table with information from most recent CourseLeaf import.
CREATE OR ALTER PROCEDURE update_current_roles
AS
BEGIN
	SET NOCOUNT ON

	-- Current date to be used for timestamps.
	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	-- Load id of most recent import to only get the newest data.
	DECLARE @LOADID INT;
	SET @LOADID = (SELECT dbo.get_max_load_id());

	-- Look for new role records that need to be inserted.
	INSERT INTO
		current_roles (
			uin,
			role,
			sequence_number,
			dept_no,
			dept,
			role_title,
			role_begin_date,
			role_end_date,
			insert_timestamp
		)
	SELECT
		courseleaf_roles.uin,
		courseleaf_roles.role,
		1,
		courseleaf_roles.dept_no,
		courseleaf_roles.dept,
		courseleaf_roles.role_title,
		@DATE,
		NULL,
		@DATE
	FROM
		courseleaf_roles
	WHERE
		NOT EXISTS (
			SELECT
				current_roles.role,
				current_roles.uin,
				current_roles.sequence_number
			FROM
				current_roles
			WHERE
				current_roles.role = courseleaf_roles.role
				AND current_roles.uin = courseleaf_roles.uin
				AND current_roles.sequence_number = 1
		)
		AND courseleaf_roles.load_id = @LOADID
	;

	-- Look for previously deactivated roles that need to be re-activated.
	-- This will insert a new row with the same UIN and role, but with a sequence
	-- number incremented by 1.
	INSERT INTO
		current_roles (
			uin,
			role,
			sequence_number,
			dept_no,
			dept,
			role_title,
			role_begin_date,
			role_end_date,
			insert_timestamp
		)
	SELECT
		uin,
		role,
		(
		SELECT
			current_roles.sequence_number + 1
		FROM
			current_roles
		WHERE
			current_roles.uin = courseleaf_roles.uin
			AND current_roles.role = courseleaf_roles.role
			AND current_roles.sequence_number = (
				SELECT
					MAX(max_seq_no.sequence_number)
				FROM
					current_roles max_seq_no
				WHERE
					max_seq_no.uin = current_roles.uin
					AND max_seq_no.role = current_roles.role
			)
			AND current_roles.role_end_date IS NOT NULL
		),
		dept_no,
		dept,
		role_title,
		@DATE,
		NULL,
		@DATE
	FROM
		courseleaf_roles
	WHERE
		EXISTS (
			SELECT
				current_roles.uin,
				current_roles.role,
				current_roles.sequence_number
			FROM
				current_roles
			WHERE
				current_roles.uin = courseleaf_roles.uin
				AND current_roles.role = courseleaf_roles.role
				AND current_roles.sequence_number = (
					SELECT
						MAX(max_seq_no.sequence_number)
					FROM
						current_roles max_seq_no
					WHERE
						max_seq_no.uin = current_roles.uin
						AND max_seq_no.role = current_roles.role
				)
				AND current_roles.role_end_date IS NOT NULL
		)
		AND courseleaf_roles.load_id = @LOADID
	;

	-- Identify any roles that were not in the CourseLeaf import.
	SELECT
		uin,
		role,
		sequence_number
	INTO
		#old_roles
	FROM
		current_roles
	WHERE
		NOT EXISTS (
			SELECT
				courseleaf_roles.uin,
				courseleaf_roles.role
			FROM
				courseleaf_roles
			WHERE
				current_roles.uin = courseleaf_roles.uin
				AND current_roles.role = courseleaf_roles.role
				AND courseleaf_roles.load_id = @LOADID
		)
		AND current_roles.role_end_date IS NULL -- Only look for active roles to end.
	;

	-- Deactivate roles not present in CourseLeaf import.
	UPDATE
		current_roles
	SET
		role_end_date = @DATE,
		modified_timestamp = @DATE
	FROM
		#old_roles old_roles
	WHERE
		current_roles.uin = old_roles.uin
		AND current_roles.role = old_roles.role
		AND current_roles.sequence_number = old_roles.sequence_number
	;

END
GO

CREATE OR ALTER PROCEDURE update_current_subjects
AS
BEGIN
	SET NOCOUNT ON

	-- Current date to be used for timestamps.
	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	-- Load id of most recent import to only get the newest data.
	DECLARE @LOADID INT;
	SET @LOADID = (SELECT dbo.get_max_load_id());

	-- Check for new subject codes that need to be inserted.
	INSERT INTO
		current_subjects (
			subject_code,
			subject,
			subject_name_codebook,
			insert_timestamp
		)
	SELECT
		banner_subjects.subject_code,
		banner_subjects.subject,
		banner_subjects.subject_name_codebook,
		@DATE
	FROM
		banner_subjects
	WHERE
		NOT EXISTS (
			SELECT
				current_subjects.subject_code
			FROM
				current_subjects
			WHERE
				current_subjects.subject_code = banner_subjects.subject_code
		)
		AND banner_subjects.load_id = @LOADID
	;

	-- Check whether any existing subjects need to have their names updated.
	UPDATE
		current_subjects
	SET
		current_subjects.subject = banner_subjects.subject,
		current_subjects.subject_name_codebook = banner_subjects.subject_name_codebook,
		current_subjects.modified_timestamp = @DATE
	FROM
		banner_subjects
	WHERE
		current_subjects.subject_code = banner_subjects.subject_code 
		AND (
			current_subjects.subject <> banner_subjects.subject
			OR current_subjects.subject_name_codebook <> banner_subjects.subject_name_codebook
		)
		AND banner_subjects.load_id = @LOADID
	;

	-- Identify any subject codes that were not in the most recent Banner import.
	SELECT
		current_subjects.subject_code
	INTO
		#old_subjects
	FROM
		current_subjects
	WHERE
		NOT EXISTS (
			SELECT
				banner_subjects.subject_code
			FROM
				banner_subjects
			WHERE
				banner_subjects.subject_code = current_subjects.subject_code
				AND banner_subjects.load_id = @LOADID
		)
	;

	DELETE
	FROM
		current_subjects
	WHERE
		current_subjects.subject_code IN (SELECT subject_code FROM #old_subjects WHERE subject_code = current_subjects.subject_code)
	;

END
GO

CREATE OR ALTER PROCEDURE update_current_terms
AS
BEGIN
	SET NOCOUNT ON

	-- Current date to be used for timestamps.
	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	-- Load id of most recent import to only get the newest data.
	DECLARE @LOADID INT;
	SET @LOADID = (SELECT dbo.get_max_load_id());

	-- Insert any terms that aren't already present in the current table.
	INSERT INTO
		current_terms (
			term_code,
			term_name_full,
			term_name,
			term_start_date,
			term_end_date,
			insert_timestamp
		)
	SELECT
		banner_terms.term_code,
		banner_terms.term_name_full,
		banner_terms.term_name,
		banner_terms.term_start_date,
		banner_terms.term_end_date,
		@DATE
	FROM
		banner_terms
	WHERE
		NOT EXISTS (
			SELECT
				term_code
			FROM
				current_terms
			WHERE
				current_terms.term_code = banner_terms.term_code
		)
		AND banner_terms.load_id = @LOADID
	;

	-- Look for any terms where dates may have changed.
	UPDATE
		current_terms
	SET
		current_terms.term_start_date = banner_terms.term_start_date,
		current_terms.term_end_date = banner_terms.term_end_date,
		current_terms.modified_timestamp = @DATE
	FROM
		banner_terms
	WHERE
		current_terms.term_code = banner_terms.term_code
		AND (
			current_terms.term_start_date <> banner_terms.term_start_date
			OR current_terms.term_end_date <> banner_terms.term_end_date
		)
		AND banner_terms.load_id = @LOADID
	;

	-- Term records should never be deleted from the current table.
	-- How would one even go about deleting the past?
END
GO

CREATE OR ALTER PROCEDURE update_current_users
AS
BEGIN
	SET NOCOUNT ON
	-- Remember that banner_userinfo gets updated by the Azure Function data load process AFTER
	-- courseleaf_users has been populated.

	-- Current date to be used for timestamps.
	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	-- Load id of most recent import to only get the newest data.
	DECLARE @LOADID INT;
	SET @LOADID = (SELECT dbo.get_max_load_id());

	-- Insert any users who do not have a UIN in the current users table.
	INSERT INTO
		current_users ( 
			uin, 
			last_name, 
			first_name, 
			email, 
			insert_timestamp
		)
	SELECT
		banner_userinfo.uin,
		-- Incorporate name suffix into last name if present.
		CASE
			WHEN banner_userinfo.name_suffix IS NOT NULL AND banner_userinfo.name_suffix <> '' THEN CONCAT(banner_userinfo.last_name, ' ', banner_userinfo.name_suffix)
			ELSE banner_userinfo.last_name
		END,
		-- Use preferred name instead of first name where present.
		CASE
			WHEN banner_userinfo.preferred_first_name IS NOT NULL AND banner_userinfo.preferred_first_name <> '' THEN banner_userinfo.preferred_first_name
			ELSE banner_userinfo.first_name
		END,
		banner_userinfo.email_address,
		@DATE
	FROM
		banner_userinfo
	WHERE
		NOT EXISTS (
			SELECT
				current_users.uin
			FROM
				current_users
			WHERE
				current_users.uin = banner_userinfo.uin
		)
		AND banner_userinfo.load_id = @LOADID
	;

	-- Update any user info that has changed.
	UPDATE
		current_users
	SET
		current_users.first_name = (
			CASE
				WHEN banner_userinfo.preferred_first_name IS NOT NULL AND banner_userinfo.preferred_first_name <> '' THEN banner_userinfo.preferred_first_name
				ELSE banner_userinfo.first_name
			END
		),
		current_users.last_name = (
			CASE
				WHEN banner_userinfo.name_suffix IS NOT NULL AND banner_userinfo.name_suffix <> '' THEN CONCAT(banner_userinfo.last_name, ' ', banner_userinfo.name_suffix)
				ELSE banner_userinfo.last_name
			END
		),
		current_users.email = banner_userinfo.email_address,
		modified_timestamp = @DATE
	FROM
		banner_userinfo
	WHERE
		current_users.uin = banner_userinfo.uin
		AND (
			-- First name has changed, no preferred name
			(current_users.first_name <> banner_userinfo.first_name AND (banner_userinfo.preferred_first_name IS NULL OR banner_userinfo.preferred_first_name = ''))
			-- Preferred name has changed
			OR (current_users.first_name <> banner_userinfo.preferred_first_name AND (banner_userinfo.preferred_first_name IS NOT NULL AND banner_userinfo.preferred_first_name <> ''))
			-- Last name has changed, no suffix
			OR (current_users.last_name <> banner_userinfo.last_name AND (banner_userinfo.name_suffix IS NULL OR banner_userinfo.name_suffix = '')) 
			-- Last name has changed, with suffix
			OR (current_users.last_name <> CONCAT(banner_userinfo.last_name, ' ', banner_userinfo.name_suffix) AND (banner_userinfo.name_suffix IS NOT NULL AND banner_userinfo.name_suffix <> ''))
			-- Email has changed
			OR current_users.email <> banner_userinfo.email_address
		)
		AND banner_userinfo.load_id = @LOADID
	;

	-- Identify any users who were not present in the most recent CourseLeaf import.
	SELECT
		current_users.uin
	INTO
		#old_users
	FROM
		current_users
	WHERE
		NOT EXISTS (
			SELECT
				courseleaf_users.uin
			FROM
				courseleaf_users
			WHERE
				courseleaf_users.uin = current_users.uin
				AND courseleaf_users.load_id = @LOADID 
		)
	;

	-- Delete any users who are no longer in the CourseLeaf application.
	DELETE
	FROM
		current_users
	WHERE
		current_users.uin IN (SELECT uin FROM #old_users WHERE uin = current_users.uin)
	;

END
GO

-- Create a special reference table based on the terms table in the ORMaintenance database.
-- This table tells the tool which four terms are the "active" terms to be selectable
-- in Power BI.
CREATE OR ALTER PROCEDURE update_ormaintenance_terms
AS
BEGIN
	SET NOCOUNT ON

	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	TRUNCATE TABLE [dbo].[ormaintenance_terms];

	INSERT INTO [dbo].[ormaintenance_terms] (
		current_term,
		previous_term,
		next_term,
		next_next_term,
		insert_timestamp
	)
	SELECT
		Term AS current_term,
		PrevTerm AS previous_term,
		NextTerm AS next_term,
		Next2Term AS next_next_term,
		@DATE AS insert_timestamp
	FROM
		[ORMaintenance].[dbo].[TblTerms]
	WHERE
		CURRENT_TERM = 'X'
	;

END
GO
