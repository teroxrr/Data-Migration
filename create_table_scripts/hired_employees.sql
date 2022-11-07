CREATE EXTERNAL TABLE `datalake`.`hired_employees`(
`id` INT,
`name` STRING,
`datetime` STRING,
`department_id` INT,
`job_id` INT)
STORED AS AVRO