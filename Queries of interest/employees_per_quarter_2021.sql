-- DESCRIPTION
-- This query returns the number of employees hired for each job and department in 2021 divided by quarter. The
-- table will be ordered alphabetically by department and job.


SELECT 
    dep.department, 
    jobs.job
    SUM(emp.q1) AS Q1,
    SUM(emp.q2) AS Q2,
    SUM(emp.q3) AS Q3,
    SUM(emp.q4) AS Q1

FROM 
    (SELECT
         *,
        CASE WHEN QUARTER(from_unixtime(UNIX_TIMESTAMP(datetime, "yyyy-MM-dd'T'HH:mm:ss'Z'"),"yyyy-MM-dd HH:mm:ss")) = 1 THEN 1 ELSE 0 END AS q1,
        CASE WHEN QUARTER(from_unixtime(UNIX_TIMESTAMP(datetime, "yyyy-MM-dd'T'HH:mm:ss'Z'"),"yyyy-MM-dd HH:mm:ss")) = 2 THEN 1 ELSE 0 END AS q2,
        CASE WHEN QUARTER(from_unixtime(UNIX_TIMESTAMP(datetime, "yyyy-MM-dd'T'HH:mm:ss'Z'"),"yyyy-MM-dd HH:mm:ss")) = 3 THEN 1 ELSE 0 END AS q3,
        CASE WHEN QUARTER(from_unixtime(UNIX_TIMESTAMP(datetime, "yyyy-MM-dd'T'HH:mm:ss'Z'"),"yyyy-MM-dd HH:mm:ss")) = 4 THEN 1 ELSE 0 END AS q4
    FROM `database.hired_employees`) AS emp

OUTER JOIN `database.departments` dep ON dep.id = emp.department_id
OUTER JOIN `database.jobs` jobs ON jobs.id = emp.job_id
WHERE YEAR(from_unixtime(UNIX_TIMESTAMP(emp.datetime, "yyyy-MM-dd'T'HH:mm:ss'Z'"),"yyyy-MM-dd HH:mm:ss")) = 2021
GROUP BY dep.department, jobs.job
ORDER BY dep.department, jobs.job
;