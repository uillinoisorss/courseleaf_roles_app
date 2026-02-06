-- Banner subjects
SELECT
	STVSUBJ_CODE AS subject_code,
	STVSUBJ_DESC AS subject,
	-- Again, I don't know where these correspondences are in Banner
	NULL AS dept_no
FROM
	SATURN.STVSUBJ
WHERE
	STVSUBJ_DISP_WEB_IND = 'Y'
ORDER BY
	STVSUBJ_CODE
;