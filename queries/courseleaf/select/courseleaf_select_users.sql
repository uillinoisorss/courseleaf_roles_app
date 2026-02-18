-- Users data load logic
SELECT
	userid AS uin,
	lname AS last_name,
	fname AS first_name,
	email AS email
FROM
	users
WHERE
	LENGTH(uin) = 9
;