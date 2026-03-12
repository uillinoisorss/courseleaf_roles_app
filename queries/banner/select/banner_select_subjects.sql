-- Banner subjects
SELECT
	banner_subjects.STVSUBJ_CODE AS subject_code,
	banner_subjects.STVSUBJ_DESC AS subject,
	codebook_subjects.SUBJ_NAME_FULL AS subject_name_codebook
FROM
	SATURN.STVSUBJ banner_subjects
	LEFT OUTER JOIN CODEBOOK.T_SUBJ codebook_subjects ON (
		banner_subjects.STVSUBJ_CODE = codebook_subjects.SUBJ_CD
		AND codebook_subjects.SUBJ_CHANGE_EXP_DT IS NULL
		AND codebook_subjects.SUBJ_CAMPUS_CD = '100'
	) 
WHERE
	banner_subjects.STVSUBJ_DISP_WEB_IND = 'Y'
ORDER BY
	subject_code
;