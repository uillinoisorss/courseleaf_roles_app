-- This was hastily adapted from a larger query, if that explains anything.

-- First, get all active courses effective in or before the term of interest.
WITH ActiveCourses AS
(
SELECT
	CourseInfo.SCBCRSE_Subj_Code AS Subject,
	CourseInfo.SCBCRSE_Crse_Numb AS CourseNumber,
	CourseInfo.SCBCRSE_Subj_Code||' '||CourseInfo.SCBCRSE_Crse_Numb AS Course,
	NVL(
		(SELECT
		 	CourseSyllabus.SCRSYLN_Long_Course_Title
		 FROM
		 	SATURN.SCRSYLN CourseSyllabus
		 WHERE
		 	CourseSyllabus.SCRSYLN_Subj_Code = CourseInfo.SCBCRSE_Subj_Code
		 	AND CourseSyllabus.SCRSYLN_Crse_Numb = CourseInfo.SCBCRSE_Crse_Numb
		 	AND CourseSyllabus.SCRSYLN_Term_Code_Eff =
		 	(SELECT
		 	 	MAX(SyllabusTerm.SCRSYLN_Term_Code_Eff)
		 	 FROM
		 	 	SATURN.SCRSYLN SyllabusTerm
		 	 WHERE
		 	 	SyllabusTerm.SCRSYLN_Term_Code_Eff <= CourseInfo.SCBCRSE_Eff_Term
		 	 	AND SyllabusTerm.SCRSYLN_Subj_Code = CourseSyllabus.SCRSYLN_Subj_Code
		 	 	AND SyllabusTerm.SCRSYLN_Crse_Numb = CourseSyllabus.SCRSYLN_Crse_Numb)), -- This just finds the long title of the course for the appropriate term.
		 CourseInfo.SCBCRSE_Title
	) AS CourseTitle, -- Pulls in long course title if it exists, short course title otherwise.
	CourseInfo.SCBCRSE_CSTA_Code AS Status,
	CourseInfo.SCBCRSE_Eff_Term AS EffectiveTerm,
	CourseInfo.SCBCRSE_Coll_Code AS College,
	CourseInfo.SCBCRSE_Dept_Code AS DepartmentCode,
	(SELECT
		DepartmentInfo.STVDEPT_Desc
	 FROM
	 	SATURN.STVDEPT DepartmentInfo
	 WHERE
	 	DepartmentInfo.STVDEPT_Code = CourseInfo.SCBCRSE_Dept_Code) AS Department,
	CourseInfo.SCBCRSE_Aprv_Code AS ControlCode,
	ROW_NUMBER() OVER (PARTITION BY CourseInfo.SCBCRSE_Subj_Code||' '||CourseInfo.SCBCRSE_Crse_Numb
	                   ORDER BY CourseInfo.SCBCRSE_Eff_Term DESC) AS RowNumber -- This row number will be used to only get active rows.
FROM
	SATURN.SCBCRSE CourseInfo
	LEFT OUTER JOIN SATURN.SCRSYLN CourseSyllabus ON (CourseInfo.SCBCRSE_Subj_Code = CourseSyllabus.SCRSYLN_Subj_Code
	                                                  AND CourseInfo.SCBCRSE_Crse_Numb = CourseSyllabus.SCRSYLN_Crse_Numb
	                                                  AND CourseSyllabus.SCRSYLN_Term_Code_Eff <= &TERM)
WHERE
	CourseInfo.SCBCRSE_CSTA_Code = 'A'
	AND CourseInfo.SCBCRSE_VPDI_Code = '1UIUC'
	AND CourseInfo.SCBCRSE_Eff_Term <= &TERM
),
-- Brings in Course ID for cross list joins.
SupplementalCourseInfo AS
(
SELECT
	CourseSupplement.SCBSUPP_Subj_Code AS Subject,
	CourseSupplement.SCBSUPP_Crse_Numb AS CourseNumber,
	CourseSupplement.SCBSUPP_Subj_Code||' '||CourseSupplement.SCBSUPP_Crse_Numb AS Course,
	CourseSupplement.SCBSUPP_Perm_Dist_Ind AS CourseID,
	CourseSupplement.SCBSUPP_Credit_Category_Ind AS ControlSubject,
	CourseSupplement.SCBSUPP_Eff_Term AS EffectiveTerm,
	ROW_NUMBER() OVER (PARTITION BY CourseSupplement.SCBSUPP_Subj_Code||' '||CourseSupplement.SCBSUPP_Crse_Numb
	                   ORDER BY CourseSupplement.SCBSUPP_Eff_Term DESC) AS RowNumber
FROM
	SATURN.SCBSUPP CourseSupplement
WHERE
	CourseSupplement.SCBSUPP_VPDI_Code = '1UIUC'
	AND CourseSupplement.SCBSUPP_Eff_Term <= &TERM
),
-- Brings in courses that are active term-effective for the provided term.
ActiveCoursesInTerm AS
(
SELECT
	CourseKey.SCBCRKY_Subj_Code AS Subject,
	CourseKey.SCBCRKY_Crse_Numb AS CourseNumber,
	CourseKey.SCBCRKY_Subj_Code||' '||CourseKey.SCBCRKY_Crse_Numb AS Course,
	CourseKey.SCBCRKY_Term_Code_Start AS CourseStartTerm,
	CourseKey.SCBCRKY_Term_Code_End AS CourseEndTerm
FROM
	SATURN.SCBCRKY CourseKey
WHERE
	&TERM BETWEEN CourseKey.SCBCRKY_Term_Code_Start AND CourseKey.SCBCRKY_Term_Code_End
	AND CourseKey.SCBCRKY_VPDI_Code = '1UIUC'
),
-- Combine the above CTEs to get "one row per course."
AllCourseInfo AS 
(
SELECT
	ActiveCourses.Subject AS Subject,
	ActiveCourses.CourseNumber AS CourseNumber,
	ActiveCourses.Course AS Course,
	ActiveCourses.CourseTitle AS CourseTitle,
	ActiveCourses.College AS College,
	ActiveCourses.DepartmentCode AS Department,
	ActiveCourses.ControlCode AS ControlCode,
	SupplementalCourseInfo.CourseID AS CourseID,
	ActiveCoursesInTerm.CourseStartTerm AS CourseStartTerm,
	ActiveCoursesInTerm.CourseEndTerm AS CourseEndTerm,
	ActiveCourses.EffectiveTerm AS CourseEffectiveTerm,
	ActiveCourses.Status AS Status
FROM
	ActiveCourses
	JOIN SupplementalCourseInfo ON ActiveCourses.Course = SupplementalCourseInfo.Course
	JOIN ActiveCoursesInTerm ON ActiveCourses.Course = ActiveCoursesInTerm.Course
WHERE
	ActiveCourses.RowNumber = 1
	AND SupplementalCourseInfo.RowNumber = 1
),
banner_courses AS
(
SELECT
	Course AS course,
	Subject AS subject_code,
	CourseNumber AS course_no,
	CourseTitle AS course_title,
	College AS college,
	Department AS dept_no,
	ControlCode AS control_code,
	CourseID AS course_id,
	CourseStartTerm AS course_start_term,
	CourseEndTerm AS course_end_term,
	CourseEffectiveTerm AS course_effective_term,
	Status AS status
FROM
	AllCourseInfo
)
SELECT
	*
FROM
	banner_courses
ORDER BY
	course
;