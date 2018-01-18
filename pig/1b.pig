--1b) Find top 5 job titles who are having highest avg growth in applications.[ALL]

--loading the cleansed data into bag

loaddata = LOAD  '/home/hduser/Desktop/h1b'  USING PigStorage('\t')  AS  (slno:int,case_status:chararray , employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray ,prevailing_wage:int ,year:chararray, worksite:chararray ,longitute:double,latitute:double); 

--filtering by each year and storing it in respective bags

filtered= filter loaddata  by $7 == '2011'; --filtering dataset by year
a= group filtered by $4;								
bag11= foreach a generate group,COUNT($1);				

filtered= filter loaddata  by $7 == '2012'; --filtering dataset by year
a= group filtered by $4;								
bag12= foreach a generate group,COUNT($1);				


filtered= filter loaddata  by $7 == '2013'; --filtering dataset by year
a= group filtered by $4;								
bag13= foreach a generate group,COUNT($1);				


filtered= filter loaddata  by $7 == '2014'; --filtering dataset by year
a= group filtered by $4;								
bag14= foreach a generate group,COUNT($1);				

filtered= filter loaddata  by $7 == '2015'; --filtering dataset by year
a= group filtered by $4;								
bag15= foreach a generate group,COUNT($1);				


filtered= filter loaddata  by $7 == '2016'; --filtering dataset by year
a= group filtered by $4;								
bag16= foreach a generate group,COUNT($1);				


--joining all the filtered bags into one

joined= join bag11 by $0,bag12 by $0,bag13 by $0,bag14 by $0,bag15 by $0,bag16 by $0;
yearly= foreach joined generate $0,$1,$3,$5,$7,$9,$11;

--applying progressive growth formula to the joined bag

growth= foreach yearly  generate $0,
(float)($2-$1)*100/$1,(float)($3-$2)*100/$2,
(float)($4-$3)*100/$3,(float)($5-$4)*100/$4,
(float)($6-$5)*100/$5;

--finding average progressive growth

avggrowth= foreach growth generate $0,($1+$2+$3+$4+$5)/5;

--ordering the results in descending order

orderedavggrowth= order avggrowth by $1 desc;

topavggrowth = limit orderedavggrowth  5;

--printing the results

dump topavggrowth;


