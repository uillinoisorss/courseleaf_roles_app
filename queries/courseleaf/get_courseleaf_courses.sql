-- Course data load logic
SELECT
	code AS course,
	dept AS subject_code,
	SUBSTR(code, LENGTH(dept) + 2) AS course_no, -- +2 excludes space between subject code and course no
	title
FROM
	course
WHERE
	-- This excludes a single completely null row.
	code <> ''
	-- This excludes a handful of rows with a department code but no course number or title.
	AND title <> ''
	-- I found a small handful of courses (5 out of 13,406) where the subject
	-- code in the "code" column did not match the subject code in the "dept"
	-- column. This filter excludes those courses since I'm not sure what to do
	-- with them and they seem more like data artifacts than live datapoints.
	AND SUBSTR(code, 1, LENGTH(dept)) = dept
ORDER BY
	code
;