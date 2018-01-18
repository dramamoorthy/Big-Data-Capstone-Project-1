--9) Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed
-- 1000 OR more than 1000) ?

--Loading the cleansed data into a bag

loaddata = LOAD  '/home/hduser/Desktop/h1b'  USING PigStorage('\t')  AS  (slno:int,case_status:chararray , employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray ,prevailing_wage:int ,year:chararray, worksite:chararray ,longitute:double,latitute:double);  

--grouping the data bag by the employer name

grouping_all = group loaddata by $2;

--counting the number of application for each employer

total_petitions = foreach grouping_all generate group, COUNT(loaddata.$1);

--filtering by the case status CERTIFIED and CERTIFIED-WITHDRAWN

filter_by_status = filter loaddata by $1 == 'CERTIFIED' OR $1 == 'CERTIFIED-WITHDRAWN';

--grouping the filtered bag by employer name

grouping_all = group filter_by_status by employer_name;

--counting the number of applications for each employer

total_certified_petition = foreach grouping_all generate group,COUNT(filter_by_status);

--joining the totals from previous two bags

joined = join total_certified_petition by $0, total_petitions by $0;

joined = foreach joined generate $0,$1,$3;

--applying the formula for success rate

op1 = foreach joined generate $0,(float)$1*100/($2),$2;

--filtering with the required conditions

op2 = filter op1 by $1>70 and $2>1000;

op3 = order op2 by $1 Desc;

--printing the output

dump op3;


