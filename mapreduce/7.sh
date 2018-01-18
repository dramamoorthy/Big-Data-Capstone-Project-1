
hadoop fs -rm -r -f /bdprj7/
hadoop jar proj1.jar bdprj7 /bdprjdata/h1b /bdprj7/

echo -e "the most popular top 10 job positions for H1B visa applications for each year for certified applicants \n\n"
hadoop fs -cat /bdprj7/p*
display /home/hduser/Desktop/project/output/7CHART.png


