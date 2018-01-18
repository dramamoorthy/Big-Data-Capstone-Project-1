#11) Export result for question no 10 to MySql database.

hadoop fs -rm -r -f /bdprj

hadoop fs -mkdir -p /bdprj

hadoop fs -put /home/hduser/Desktop/pig10sol/ /bdprj/

mysql -u root -p'1' -e 'drop database bdprj;create database if not exists bdprj;use bdprj;create table q11(job_title varchar(100),success_rate float,total_petitions int);';

sqoop export --connect jdbc:mysql://localhost/bdprj --username root --password '1' --table q11 --update-mode allowinsert  --export-dir /bdprj/p* --input-fields-terminated-by '\t' ;

mysql -u root -p'1' -e 'select * from bdprj.q11';



