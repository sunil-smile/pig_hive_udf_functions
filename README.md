# pig_hive_udf_functions
REGISTER /home/cloudera/simplyanalytics/pig_demo-0.0.1-SNAPSHOT.jar;
A = LOAD '/tmp/simplyanalytics/pig/employee.txt' USING com.simplyanalytics.pigapp.customloader.CustomLoaderOnlySchema('/tmp/simplyanalytics/pig/employee.schema',',');
describe A;
dump A;
B = foreach A generate $1,$3;
C = foreach A generate emplocation,empname,empid;
A = LOAD '/tmp/simplyanalytics/pig/employee.txt' USING com.simplyanalytics.pigapp.customloader.CustomLoaderWithSchema('/tmp/simplyanalytics/pig/employee.schema',',','0','1','5');


DEFINE com.simplyanalytics.pigapp.md5.Md5Hash md5();

result = foreach A generate FLATTEN(md5(TOTUPLE(*)));

result = foreach A generate com.simplyanalytics.pigapp.md5.Md5Hash(*);




CREATE TABLE IF NOT EXISTS employee ( eid int, empname String,
empdesc String, empbranch String,empdept String,emplocation String,ts1 String,ts2 String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/cloudera/simplyanalytics/pig/employee.txt' OVERWRITE INTO TABLE employee;


add jar /home/cloudera/simplyanalytics/pig_demo-0.0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION MaxTS AS 'com.simplyanalytics.hiveapp.udf.MaxTimeStamp';
CREATE TEMPORARY FUNCTION CompareTS AS 'com.simplyanalytics.hiveapp.udf.CompareTimeStamp';

Describe function CompareTS;
Describe function MaxTS;
select MaxTS(ts1) from employee;
select CompareTS(ts1,ts2) from employee;
