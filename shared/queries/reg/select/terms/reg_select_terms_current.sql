SELECT
	Term AS current_term,
	PrevTerm AS previous_term,
	NextTerm AS next_term,
	Next2Term AS next_next_term
FROM
	[ORMaintenance].[dbo].[TblTerms]
WHERE
	CURRENT_TERM = 'X'
;