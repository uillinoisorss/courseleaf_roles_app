SELECT
    ISNULL(MAX(load_id), 0) AS max_load_id
FROM
    <DATABASE>.<SCHEMA>.[courseleaf_imports]
;