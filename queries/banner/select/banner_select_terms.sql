SELECT
	STVTERM_CODE AS term_code,
	STVTERM_DESC AS term_name_full,
	CASE
		WHEN STVTERM_DESC LIKE '%Academic Year%' THEN STVTERM_DESC
		WHEN STVTERM_DESC LIKE '%Winter%' THEN SUBSTR(STVTERM_DESC, 1, INSTR(STVTERM_DESC, ' ', 8))
		ELSE SUBSTR(STVTERM_DESC, 1, INSTR(STVTERM_DESC, ' -'))
	END AS term_name
FROM
	SATURN.STVTERM
WHERE
	SUBSTR(STVTERM_CODE, 1, 1) = '1' -- only Urbana-Champaign terms
	AND STVTERM_CODE < '199999'
    AND STVTERM_CODE >= '120048' -- omit pre-Banner terms
	AND STVTERM_TRMT_CODE IN ('S','W') -- only Semester and Winter terms, excludes Academic Year terms and Global Campus terms
ORDER BY
	STVTERM_CODE
;