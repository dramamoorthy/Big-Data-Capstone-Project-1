
hadoop fs -rm -r -f /bdprj5b/
hadoop jar proj1.jar bdprj5btree /bdprjdata/h1b /bdprj5b/

echo -e "the most popular top 10 job positions for H1B visa applications for each year for certified applicants \n\n"
hadoop fs -cat /bdprj5b/p*

