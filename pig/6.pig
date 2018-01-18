--6) Find the percentage and the count of each case status on total applications for each year.  
--Create a line graph depicting the pattern of All the cases over the period of time.

--loading the cleansed data into a bag

loaddata = LOAD  '/home/hduser/Desktop/h1b'  USING PigStorage('\t')  AS  (slno:int,case_status:chararray , employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray ,prevailing_wage:int ,year:chararray, worksite:chararray ,longitute:double,latitute:double);           	                          

--grouping the loaded bag by year

group_by_year = group loaddata by $7;

--finding total number of applications for each year with the grouped by year bag

year_total = foreach group_by_year generate group, COUNT(loaddata.$1);

--grouping the loaded bad by both year and case status

group_by_year_status = group loaddata by ($7,$1);

--finding total number of applications for each case status for each year

year_status_total = foreach group_by_year_status generate group,group.$0,COUNT($1);

--Joining the two totals generated before into a single bag to perform the math operation

joined = join year_status_total by $1, year_total by $0;

--now we have both the totals required for the calculation and the percentage is calculated.

percentage = foreach joined generate FLATTEN($0),ROUND_TO((float)($2*100)/$4,2),$2;

--then the output is printed.
dump percentage;






