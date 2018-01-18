
hadoop fs -rm -r -f /bdprj5a/
hadoop jar proj1.jar bdprj5atree /bdprjdata/h1b /bdprj5a/

echo -e "the most popular top 10 job positions for H1B visa applications for each year for all applicants \n\n"
hadoop fs -cat /bdprj5a/p*

