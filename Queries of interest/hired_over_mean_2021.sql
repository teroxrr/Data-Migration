-- DESCRIPTION
-- This query returns a list of ids, name and number of employees hired of each department that hired more
-- employees than the mean of employees hired in 2021 for all the departments, ordered
-- by the number of employees hired (descending).

SELECT
    id, 
    department,
    hired 

FROM
    (SELECT
        dep.id,
        dep.department,
        COUNT(emp.id) AS hired
    FROM `database.departments` dep
    LEFT JOIN `database.hired_employees` emp ON emp.department_id = dep.id
    WHERE 
        YEAR(from_unixtime(UNIX_TIMESTAMP(emp.datetime, "yyyy-MM-dd'T'HH:mm:ss'Z'"),"yyyy-MM-dd HH:mm:ss")) = 2021
    GROUP BY dep.id, dep.department)

WHERE hired > (SELECT COUNT(id) / COUNT(DISTINCT department_id)
               FROM `database.hired_employees`
               WHERE YEAR(from_unixtime(UNIX_TIMESTAMP(`datetime`, "yyyy-MM-dd'T'HH:mm:ss'Z'"),"yyyy-MM-dd HH:mm:ss")) = 2021)

ORDER BY hired DESC;