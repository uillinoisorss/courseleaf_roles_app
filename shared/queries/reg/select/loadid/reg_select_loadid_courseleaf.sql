SELECT
    ISNULL(MAX(load_id), 0) AS max_load_id
FROM
    [CourseLeaf_Contacts].[dbo].[courseleaf_imports]
;