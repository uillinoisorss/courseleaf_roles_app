-- Base role data logic
SELECT 
        s1.value as name, s2.value as members, s3.value as email
    FROM 
        pages
        JOIN tcdata using(pagekey)
        JOIN tcval s1 using(tckey)
        JOIN tcval s2 using (tckey,rank)
        JOIN tcval s3 using (tckey,rank)
    WHERE 
        pages.path = '/courseleaf/roles.html' AND tcdata.tctype = 'tcf'  
        AND s1.part= 'name' AND s2.part= 'members' AND s3.part = 'email'
;