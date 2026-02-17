WITH original_departments AS
(
SELECT DISTINCT
	department AS dept_no,
	(
	SELECT
		stvdept_desc
	FROM
		stvdept_orig
	WHERE
		cimlookup.department = stvdept_orig.stvdept_code
	) AS dept_name, 
	(
	SELECT 
		college 
	FROM 
		cimlookup cl2 
	WHERE 
		cimlookup.department = cl2.department 
		AND college IS NOT NULL
	) AS college
FROM
	cimlookup
WHERE
	department IS NOT NULL
ORDER BY
	department
),
override_departments AS
(
SELECT DISTINCT
	stvdept_code AS dept_no,
	(
	SELECT
		override2.newval
	FROM
		stvdept_oride override2
	WHERE
		override2.stvdept_code = stvdept_oride.stvdept_code
		AND override2.fieldname = 'stvdept_desc'
	) AS dept_name
FROM
	stvdept_oride
)
SELECT
	orig.dept_no AS dept_no,
	CASE
		WHEN oride.dept_name IS NOT NULL THEN oride.dept_name
		WHEN orig.dept_name IS NOT NULL THEN orig.dept_name
		-- This clause was included to handle a single case (1787: Translation Studies) where
		-- the department name was in neither stvdept_orig nor stvdept_oride, only stvdept (in SQLite).
		-- Not sure why this or if there will be others. Annoying.
		ELSE (
			SELECT
				stvdept_desc
			FROM
				stvdept
			WHERE
				stvdept_code = orig.dept_no
		)
	END AS dept_name,
	orig.college AS college,
	(
	SELECT
		codedesc.name
	FROM
		codedesc
	WHERE
		codedesc.setname = 'college'
		AND codedesc.code = orig.college
	) AS college_name
FROM
	original_departments orig
	LEFT JOIN override_departments oride ON orig.dept_no = oride.dept_no
ORDER BY
	orig.dept_no
;