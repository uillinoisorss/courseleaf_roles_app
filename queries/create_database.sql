-- Run this code to create the CourseLeaf application database from scratch.
-- This will delete and recreate everything in the process, so only run if
-- you aren't worried about losing any data.

USE [CourseLeaf_Contacts];
GO

---------------------------------------------------------------------------------------------------
-- TABLES
---------------------------------------------------------------------------------------------------

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
  [college] nvarchar(2),
  [college_name] nvarchar(255),
  [insert_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [banner_imports];
GO

CREATE TABLE [banner_imports] (
  [load_id] int PRIMARY KEY IDENTITY(1, 1),
  [load_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [banner_subjects];
GO

CREATE TABLE [banner_subjects] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [load_id] int,
  [subject_code] nvarchar(50),
  [subject] nvarchar(50),
  [dept_no] nvarchar(4),
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

DROP TABLE IF EXISTS [banner_user_info];
GO

CREATE TABLE [banner_user_info] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [load_id] int,
  [uin] nvarchar(9),
  [last_name] nvarchar(50),
  [first_name] nvarchar(50),
  [email] nvarchar(50),
  [insert_timestamp] datetime2(7)
)
GO

-- Create CourseLeaf tables

DROP TABLE IF EXISTS [courseleaf_courses];
GO

CREATE TABLE [courseleaf_courses] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [load_id] int NOT NULL,
  [course] nvarchar(50) NOT NULL,
  [subject_code] nvarchar(10) NOT NULL,
  [course_no] nvarchar(3) NOT NULL,
  [course_title] nvarchar(255),
  [insert_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [courseleaf_departments];
GO

CREATE TABLE [courseleaf_departments] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [load_id] int,
  [dept_no] nvarchar(4),
  [dept_name] nvarchar(255),
  [college] nvarchar(2),
  [college_name] nvarchar(255),
  [insert_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [courseleaf_imports];
GO

CREATE TABLE [courseleaf_imports] (
  [load_id] int PRIMARY KEY IDENTITY(1, 1),
  [load_timestamp] datetime2(7)
)
GO

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

DROP TABLE IF EXISTS [courseleaf_subjects];
GO

CREATE TABLE [courseleaf_subjects] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [load_id] int,
  [subject_code] nvarchar(50),
  [subject] nvarchar(50),
  [dept_no] nvarchar(4),
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
  [college] nvarchar(2),
  [college_name] nvarchar(255),
  [insert_timestamp] datetime2(7),
  [modified_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [current_roles];
GO

CREATE TABLE [current_roles] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [role] nvarchar(50),
  [dept_no] nvarchar(4),
  [dept] nvarchar(50),
  [role_title] nvarchar(50),
  [uin] nvarchar(9),
  [role_begin_date] date,
  [role_end_date] date,
  [insert_timestamp] datetime2(7),
  [modified_timestamp] datetime2(7)
)
GO

DROP TABLE IF EXISTS [current_subjects];
GO

CREATE TABLE [current_subjects] (
  [subject_code] nvarchar(50) PRIMARY KEY,
  [subject] nvarchar(50),
  [dept_no] nvarchar(4),
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

-- Create PowerBI tables
-- This table is probably not going to be used for anything so delete it later.

DROP TABLE IF EXISTS [powerbi_crosslists];
GO

CREATE TABLE [powerbi_crosslists] (
  [id] int PRIMARY KEY IDENTITY(1, 1),
  [selected_course] nvarchar(50),
  [controlling_course] nvarchar(50),
  [courses_in_group] nvarchar(50),
  [is_controlling] nvarchar(1)
)

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

---------------------------------------------------------------------------------------------------
-- STORED PROCEDURES
---------------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS [dbo].[deactivate_old_roles];
GO

CREATE PROCEDURE deactivate_old_roles
AS
BEGIN
	
	DECLARE @DATE datetime2(7);
	SET @DATE = GETDATE();

	SELECT
		uin,
		role
	INTO
		#old_roles
	FROM
		current_roles
	WHERE
		NOT EXISTS (
			SELECT
				*
			FROM
				courseleaf_roles
			WHERE
				current_roles.uin = courseleaf_roles.uin
				AND current_roles.role = courseleaf_roles.role
				AND courseleaf_roles.load_id = (
					SELECT
						MAX(LOAD_ID)
					FROM
						courseleaf_roles
				)
		)
	;

	UPDATE
		current_roles
	SET
		role_end_date = @DATE,
		modified_timestamp = @DATE
	WHERE
		current_roles.uin IN (SELECT uin FROM #old_roles WHERE role = current_roles.role)
		AND current_roles.role IN (SELECT role FROM #old_roles WHERE uin = current_roles.uin)
	;
END

DROP PROCEDURE IF EXISTS [dbo].[generate_powerbi_crosslists];
GO

CREATE PROCEDURE generate_powerbi_crosslists
AS
BEGIN
	SET NOCOUNT ON;

    TRUNCATE TABLE dbo.powerbi_crosslists;

	WITH crosslists AS
	(
	SELECT
		course,
		controlling_course
	FROM
		current_crosslists
	),
	is_not_self AS
	(
	SELECT
		crosslists.course AS selected_course,
		crosslists.controlling_course AS controlling_course,
		courses.course AS courses_in_group
	FROM
		crosslists
		JOIN crosslists courses ON (
			crosslists.course <> courses.course
			AND crosslists.controlling_course = courses.controlling_course
		)
	WHERE
		crosslists.controlling_course IS NOT NULL
	),
	is_self AS
	(
	SELECT
		crosslists.course AS selected_course,
		crosslists.controlling_course AS controlling_course,
		courses.course AS courses_in_group
	FROM
		crosslists
		JOIN crosslists courses ON (
			crosslists.course = courses.course
			AND crosslists.controlling_course = courses.controlling_course
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
		dbo.powerbi_crosslists (selected_course, controlling_course, courses_in_group, is_controlling)
	SELECT
		all_courses.*,
		CASE
			WHEN controlling_course = courses_in_group THEN 'Y'
			ELSE 'N'
		END as controlling
	FROM
		all_courses
	ORDER BY
		controlling_course,
		selected_course,
		courses_in_group
	;
END
GO

-- Crosslists

DROP PROCEDURE IF EXISTS [dbo].[update_current_crosslists];
GO

CREATE PROCEDURE [dbo].[update_current_crosslists]
AS
BEGIN
	SET NOCOUNT ON
	-- Creates crosslist correspondence table: 'course' is the course that gets looked up,
	-- 'controlling_course' is the course that controls the course that gets looked up.

	TRUNCATE TABLE [dbo].[current_crosslists];

	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	INSERT INTO [dbo].[current_crosslists] (
		crosslist_effective_term,
		course,
		controlling_course,
		is_crosslisted,
		is_controlling,
		insert_timestamp
	)
	-- Crosslisted courses that are non-controlling get listed alongside their controlling course here
	SELECT
		non_controlling.term AS crosslist_effective_term,
		non_controlling.course AS course,
		controlling.course AS controlling_course,
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
		'N' AS is_crosslisted,
		'Y' AS is_controlling,
		@DATE AS insert_timestamp
	FROM
		current_courses
	WHERE
		current_courses.control_code = ''
	;
END
GO

DROP PROCEDURE IF EXISTS [dbo].[update_ormaintenance_terms];
GO

CREATE PROCEDURE [dbo].[update_ormaintenance_terms]
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

---------------------------------------------------------------------------------------------------
-- TRIGGERS
---------------------------------------------------------------------------------------------------

-- Courses

DROP TRIGGER IF EXISTS [dbo].[update_current_courses];
GO

CREATE TRIGGER [dbo].[update_current_courses]
ON [dbo].[banner_courses]
AFTER INSERT
NOT FOR REPLICATION
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	IF EXISTS (
		SELECT
			inserted.term,
			inserted.course,
			inserted.course_id,
			inserted.course_effective_term
		FROM
			inserted
		WHERE
			NOT EXISTS (
				SELECT
					current_courses.term,
					current_courses.course,
					current_courses.course_id,
					current_courses.course_effective_term
				FROM
					current_courses
				WHERE
					current_courses.term = inserted.term
					AND current_courses.course = inserted.course
					AND current_courses.course_id = inserted.course_id
					AND current_courses.course_effective_term = inserted.course_effective_term
			)
	)
	BEGIN
		INSERT INTO
			[dbo].[current_courses] (
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
			inserted.term,
			inserted.course,
			inserted.subject_code,
			inserted.course_no,
			inserted.course_title,
			inserted.college,
			inserted.dept_no,
			inserted.control_code,
			inserted.course_id,
			inserted.course_start_term,
			inserted.course_end_term,
			inserted.course_effective_term,
			inserted.status,
			@DATE,
			@DATE
		FROM
			inserted
		WHERE
			NOT EXISTS (
				SELECT
					current_courses.term,
					current_courses.course,
					current_courses.course_id,
					current_courses.course_effective_term
				FROM
					current_courses
				WHERE
					current_courses.term = inserted.term
					AND current_courses.course = inserted.course
					AND current_courses.course_id = inserted.course_id
					AND current_courses.course_effective_term = inserted.course_effective_term
			)
	END
END
GO

-- Departments

DROP TRIGGER IF EXISTS [dbo].[update_current_departments];
GO

CREATE TRIGGER [dbo].[update_current_departments]
ON [dbo].[courseleaf_departments]
AFTER INSERT
NOT FOR REPLICATION
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	INSERT INTO
		[dbo].[current_departments] (dept_no, dept_name, college, college_name, insert_timestamp)
	SELECT
		inserted.dept_no,
		inserted.dept_name,
		inserted.college,
		inserted.college_name,
		inserted.insert_timestamp
	FROM
		inserted
	;	
END
GO

-- Roles

DROP TRIGGER IF EXISTS [dbo].[update_current_roles];
GO

CREATE TRIGGER [dbo].[update_current_roles]
ON [dbo].[courseleaf_roles]
AFTER INSERT
NOT FOR REPLICATION
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	IF EXISTS (
		SELECT
			inserted.role,
			inserted.uin
		FROM
			inserted
		WHERE
			NOT EXISTS (
				SELECT
					current_roles.role,
					current_roles.uin
				FROM
					current_roles
				WHERE
					current_roles.role = inserted.role
					AND current_roles.uin = inserted.uin
			)
	)
	BEGIN
		INSERT INTO
			[dbo].[current_roles] (
				role,
				dept_no,
				dept,
				role_title,
				uin,
				role_begin_date,
				role_end_date,
				insert_timestamp,
				modified_timestamp
			)
		SELECT
			inserted.role,
			inserted.dept_no,
			inserted.dept,
			inserted.role_title,
			inserted.uin,
			@DATE,
			NULL,
			@DATE,
			@DATE
		FROM
			inserted
		WHERE
			NOT EXISTS (
				SELECT
					current_roles.role,
					current_roles.uin
				FROM
					current_roles
				WHERE
					current_roles.role = inserted.role
					AND current_roles.uin = inserted.uin
			)
		;
	END
END
GO

-- Subjects

DROP TRIGGER IF EXISTS [dbo].[update_current_subjects];
GO

CREATE TRIGGER [dbo].[update_current_subjects]
ON [dbo].[courseleaf_subjects]
AFTER INSERT
NOT FOR REPLICATION
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	INSERT INTO
		[dbo].[current_subjects] (
			subject_code,
			subject,
			dept_no,
			insert_timestamp
		)
	SELECT
		inserted.subject_code,
		inserted.subject,
		inserted.dept_no,
		@DATE
	FROM
		inserted
	;
END
GO

-- Terms

DROP TRIGGER IF EXISTS [dbo].[update_current_terms];
GO

CREATE TRIGGER [dbo].[update_current_terms]
ON [dbo].[banner_terms]
AFTER INSERT
NOT FOR REPLICATION
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	INSERT INTO
		[dbo].[current_terms] (
			term_code,
			term_name_full,
			term_name,
			term_start_date,
			term_end_date,
			insert_timestamp
		)
	SELECT
		inserted.term_code,
		inserted.term_name_full,
		inserted.term_name,
		inserted.term_start_date,
		inserted.term_end_date,
		@DATE
	FROM
		inserted
	WHERE
		NOT EXISTS (
			SELECT
				term_code
			FROM
				current_terms
			WHERE
				current_terms.term_code = inserted.term_code
	)
	;
END
GO

-- Users

DROP TRIGGER IF EXISTS [dbo].[update_current_users];
GO

CREATE TRIGGER [dbo].[update_current_users]
ON [dbo].[courseleaf_users]
AFTER INSERT
NOT FOR REPLICATION
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @DATE DATETIME2(7);
	SET @DATE = GETDATE();

	INSERT INTO
		[dbo].[current_users] ( 
			uin, 
			last_name, 
			first_name, 
			email, 
			insert_timestamp
		)
	SELECT
		inserted.uin,
		inserted.last_name,
		inserted.first_name,
		inserted.email,
		@DATE
	FROM
		inserted
	;
END
GO