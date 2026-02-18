-- Banner departments
SELECT
	STVDEPT_CODE AS dept_no,
	STVDEPT_DESC AS dept_name,
	-- I don't know how to get college/dept correlation in Banner
	-- so I'm leaving these blank for now.
	NULL AS college,
	NULL AS college_name
FROM
	SATURN.STVDEPT
WHERE
	SUBSTR(STVDEPT_CODE, 1, 1) = '1'
ORDER BY
	STVDEPT_CODE
;