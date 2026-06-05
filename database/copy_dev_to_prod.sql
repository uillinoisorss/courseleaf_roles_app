INSERT INTO
	CourseLeaf_Contacts.dbo.banner_courses (load_id, term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status, insert_timestamp)
SELECT
	load_id, term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status, insert_timestamp
FROM
	courseleaf_dev.dbo.banner_courses
;

INSERT INTO
	CourseLeaf_Contacts.dbo.banner_departments (load_id, dept_no, dept_name, insert_timestamp)
SELECT
	load_id, dept_no, dept_name, insert_timestamp
FROM
	courseleaf_dev.dbo.banner_departments
;

INSERT INTO
	CourseLeaf_Contacts.dbo.banner_subjects (load_id, subject_code, subject, subject_name_codebook, insert_timestamp)
SELECT
	load_id, subject_code, subject, subject_name_codebook, insert_timestamp
FROM
	courseleaf_dev.dbo.banner_subjects
;

INSERT INTO
	CourseLeaf_Contacts.dbo.banner_terms (load_id, term_code, term_name_full, term_name, term_start_date, term_end_date, insert_timestamp)
SELECT
	load_id, term_code, term_name_full, term_name, term_start_date, term_end_date, insert_timestamp
FROM
	courseleaf_dev.dbo.banner_terms
;

INSERT INTO
	CourseLeaf_Contacts.dbo.banner_userinfo (load_id, uin, pidm, first_name, preferred_first_name, middle_name, last_name, name_suffix, email_address, netid, campus_domain, max_activity_date, insert_timestamp)
SELECT
	load_id, uin, pidm, first_name, preferred_first_name, middle_name, last_name, name_suffix, email_address, netid, campus_domain, max_activity_date, insert_timestamp
FROM
	courseleaf_dev.dbo.banner_userinfo
;

INSERT INTO
	CourseLeaf_Contacts.dbo.courseleaf_roles (load_id, role, dept_no, dept, role_title, uin, insert_timestamp)
SELECT
	load_id, role, dept_no, dept, role_title, uin, insert_timestamp
FROM
	courseleaf_dev.dbo.courseleaf_roles
;

INSERT INTO
	CourseLeaf_Contacts.dbo.courseleaf_users (load_id, uin, last_name, first_name, email, insert_timestamp)
SELECT
	load_id, uin, last_name, first_name, email, insert_timestamp
FROM
	courseleaf_dev.dbo.courseleaf_users
;

INSERT INTO
	CourseLeaf_Contacts.dbo.current_courses (term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status, insert_timestamp, modified_timestamp)
SELECT
	term, course, subject_code, course_no, course_title, college, dept_no, control_code, course_id, course_start_term, course_end_term, course_effective_term, status, insert_timestamp, modified_timestamp
FROM
	courseleaf_dev.dbo.current_courses
;

INSERT INTO
	CourseLeaf_Contacts.dbo.current_crosslists (crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling, insert_timestamp, modified_timestamp)
SELECT
	crosslist_effective_term, course, controlling_course, controlling_dept_no, is_crosslisted, is_controlling, insert_timestamp, modified_timestamp
FROM
	courseleaf_dev.dbo.current_crosslists
;

INSERT INTO
	CourseLeaf_Contacts.dbo.current_departments (dept_no, dept_name, insert_timestamp, modified_timestamp)
SELECT
	dept_no, dept_name, insert_timestamp, modified_timestamp
FROM
	courseleaf_dev.dbo.current_departments
;

INSERT INTO
	CourseLeaf_Contacts.dbo.current_roles (uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date, insert_timestamp, modified_timestamp)
SELECT
	uin, role, sequence_number, dept_no, dept, role_title, role_begin_date, role_end_date, insert_timestamp, modified_timestamp
FROM
	courseleaf_dev.dbo.current_roles
;

INSERT INTO
	CourseLeaf_Contacts.dbo.current_subjects (subject_code, subject, subject_name_codebook, insert_timestamp, modified_timestamp)
SELECT
	subject_code, subject, subject_name_codebook, insert_timestamp, modified_timestamp
FROM
	courseleaf_dev.dbo.current_subjects
;

INSERT INTO
	CourseLeaf_Contacts.dbo.current_terms (term_code, term_name_full, term_name, term_start_date, term_end_date, insert_timestamp, modified_timestamp)
SELECT
	term_code, term_name_full, term_name, term_start_date, term_end_date, insert_timestamp, modified_timestamp
FROM
	courseleaf_dev.dbo.current_terms
;

INSERT INTO
	CourseLeaf_Contacts.dbo.current_users (uin, last_name, first_name, email, insert_timestamp, modified_timestamp)
SELECT
	uin, last_name, first_name, email, insert_timestamp, modified_timestamp
FROM
	courseleaf_dev.dbo.current_users
;

INSERT INTO
	CourseLeaf_Contacts.dbo.imports (load_begin_timestamp, load_end_timestamp, banner_courses_rows_inserted, banner_departments_rows_inserted, banner_subjects_rows_inserted, banner_terms_rows_inserted, banner_userinfo_rows_inserted, courseleaf_roles_rows_inserted, courseleaf_users_rows_inserted)
SELECT
	load_begin_timestamp, load_end_timestamp, banner_courses_rows_inserted, banner_departments_rows_inserted, banner_subjects_rows_inserted, banner_terms_rows_inserted, banner_userinfo_rows_inserted, courseleaf_roles_rows_inserted, courseleaf_users_rows_inserted
FROM
	courseleaf_dev.dbo.imports
;

INSERT INTO
	CourseLeaf_Contacts.dbo.ormaintenance_terms (current_term, previous_term, next_term, next_next_term)
SELECT
	current_term, previous_term, next_term, next_next_term
FROM
	courseleaf_dev.dbo.ormaintenance_terms
;

INSERT INTO
	CourseLeaf_Contacts.dbo.powerbi_crosslists (crosslist_effective_term, selected_course, controlling_course, courses_in_group, is_controlling, is_crosslisted)
SELECT
	crosslist_effective_term, selected_course, controlling_course, courses_in_group, is_controlling, is_crosslisted
FROM
	courseleaf_dev.dbo.powerbi_crosslists
;

INSERT INTO
	CourseLeaf_Contacts.dbo.powerbi_roles (term, role, dept_no, uin, first_name, last_name, email, role_end_date, department_name)
SELECT
	term, role, dept_no, uin, first_name, last_name, email, role_end_date, department_name
FROM
	courseleaf_dev.dbo.powerbi_roles
;