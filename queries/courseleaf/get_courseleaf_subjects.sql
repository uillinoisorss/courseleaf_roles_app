-- Subject data load logic
WITH original_subjects AS
(
SELECT DISTINCT
	stvsubj_code AS subject_code
FROM
	stvsubj_orig
WHERE
	NOT EXISTS (
		SELECT
			stvsubj_code
		FROM
			stvsubj_oride
		WHERE
			stvsubj_oride.stvsubj_code = stvsubj_orig.stvsubj_code
			AND stvsubj_oride.fieldname = 'stvsubj_code'
			AND stvsubj_oride.newval IS NULL
	)
ORDER BY
	stvsubj_code
),
override_subjects AS
(
SELECT
	stvsubj_code AS subject_code
FROM
	stvsubj_oride
WHERE
	fieldname = 'stvsubj_code'
	AND oldval IS NULL
),
all_subjects AS
(
SELECT
	*
FROM
	original_subjects
UNION
SELECT
	*
FROM
	override_subjects
)
SELECT
	subject_code,
	IFNULL(
		(
		SELECT
			oride.newval
		FROM
			stvsubj_oride oride
		WHERE
			oride.stvsubj_code = all_subjects.subject_code
			AND oride.fieldname = 'stvsubj_desc'
			AND oride.newval IS NOT NULL
		),
		(
		SELECT
			orig.stvsubj_desc
		FROM
			stvsubj_orig orig
		WHERE
			orig.stvsubj_code = all_subjects.subject_code
		)
	) AS subject,
	(
	SELECT
		cimlookup.department
	FROM
		cimlookup
	WHERE
		cimlookup.subject = all_subjects.subject_code
		AND cimlookup.department IS NOT NULL
	) AS dept_no
FROM
	all_subjects
ORDER BY
	subject_code
;