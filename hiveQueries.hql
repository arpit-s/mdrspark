DROP TABLE relationDocClinic;
DROP TABLE relationDocPatient;
DROP TABLE  docDocCrossProd;
DROP TABLE clinic;
DROP TABLE docClinic;
DROP TABLE appointments;
DROP TABLE dummy;
DROP TABLE final;
SHOW TABLES;


/*CREATE TABLE clinic (id STRING,created_at STRING,updated_at STRING,deleted_at STRING,name STRING,address STRING,zip BIGINT,city STRING,location STRING,email STRING,password STRING,country_id STRING,clinic_type_id STRING,phone_country_code STRING,phone_number STRING,profile_image_id STRING,time_zone STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'adl://mdrstore.azuredatalakestore.net/sparkFiles/clinic';
*/

CREATE TABLE docClinic(id INT,created_at DATE,updated_at DATE,deleted_at DATE,title STRING,doctor_id INT,clinic_id INT,primary STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'adl://mdrstore.azuredatalakestore.net/sparkFiles/docClinic';



CREATE TABLE relationDocClinic (src int,dst int,clinic_id int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'adl://mdrstore.azuredatalakestore.net/sparkFiles/relationDocClinic';

INSERT OVERWRITE TABLE default.relationDocClinic 
SELECT t1.doctor_id,t2.doctor_id,t1.clinic_id  
FROM docClinic t1  
CROSS JOIN docClinic t2  
WHERE t1.clinic_id = t2.clinic_id AND t1.doctor_id != t2.doctor_id ;


/*
SELECT * FROM default.relationDocClinic t1
JOIN clinic t2
ON t1.clinic_id = t2.id ;
*/


CREATE TABLE appointments(
id VARCHAR(100)  , 
created_at timestamp  , 
updated_at timestamp  , 
deleted_at timestamp  ,  
patient_id VARCHAR(100) , 
user_id VARCHAR(100) , 
rescheduled_user_id VARCHAR(100) , 
rescheduled_from_id VARCHAR(100) , 
updated_by VARCHAR(100) , 
doctor_id VARCHAR(100) , 
clinic_id VARCHAR(100) , 
doctor_timetable_id VARCHAR(100) , 
appointment_status_id VARCHAR(50)  , 
appointment_type_id VARCHAR(50)  , 
start_time timestamp  , 
end_time timestamp  , 
patient_condition_id VARCHAR(50)  , 
start_at timestamp  , 
end_at timestamp  ,  
rating_comment VARCHAR(50),  
cancel_reason VARCHAR(150), 
rating VARCHAR(50)  , 
rating_at timestamp , 
booking_reason varchar(150),
user_patient_id VARCHAR(100), 
verify_attempted VARCHAR(50)  , 
note VARCHAR(100)) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'adl://mdrstore.azuredatalakestore.net/sparkFiles/appointments';




CREATE TABLE relationDocPatient (src int,dst int,patient_id int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'adl://mdrstore.azuredatalakestore.net/sparkFiles/relationDocPatient';

INSERT OVERWRITE TABLE relationDocPatient 
SELECT t1.doctor_id,t2.doctor_id,t1.patient_id  
FROM appointments t1  
CROSS JOIN appointments t2 
WHERE t1.patient_id = t2.patient_id AND t1.doctor_id != t2.doctor_id ;




--had to store as orc as merge would act on ACID table only
CREATE TABLE docDocCrossProd(src int, dst int, clinic_exist int, cnt int)
clustered by (src) into 2 buckets
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n' 
stored as orc
LOCATION 'adl://mdrstore.azuredatalakestore.net/sparkFiles/docDocCrossProd'
TBLPROPERTIES('transactional'='true');

--Changing Transaction Manager to enable ACID 
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.support.concurrency=true;
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.compactor.initiator.on=true;
SET hive.compactor.worker.threads=1;
SET hive.merge.cardinality.check=false;

INSERT INTO TABLE docDocCrossProd
SELECT t1.doctor_id,t2.doctor_id,0,0
FROM docClinic t1
CROSS JOIN docClinic t2 
WHERE t1.doctor_id != t2.doctor_id ;


MERGE INTO docDocCrossProd t1
USING relationDocClinic t2
ON t1.src = t2.src AND t1.dst = t2.dst
WHEN MATCHED THEN UPDATE SET clinic_exist = 1;

--had to make because merge was not accepting expresssions directly
CREATE TABLE dummy (src int,dst int,patient_id int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'adl://mdrstore.azuredatalakestore.net/sparkFiles/dummy';

INSERT OVERWRITE TABLE dummy 
SELECT src,dst,count(patient_id) AS countt from relationDocPatient
GROUP BY src,dst;

MERGE INTO docDocCrossProd t1
USING dummy t2
ON t1.src = t2.src AND t1.dst = t2.dst
WHEN MATCHED THEN UPDATE SET cnt = countt;


--had to make,because there's issue orc->spark dataframe
CREATE TABLE final(src int,dst int,clinic_exist int,cnt int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'adl://mdrstore.azuredatalakestore.net/sparkFiles/dummy';

INSERT INTO TABLE final
SELECT src,dst,clinic_exist,cnt
FROM docDocCrossProd;
