SELECT
	uin
FROM
	<DATABASE>.<SCHEMA>.[courseleaf_users]
WHERE
	load_id = (
		SELECT
			MAX(load_id)
		FROM
			[CourseLeaf_Contacts].[dbo].[courseleaf_users]
	)
;