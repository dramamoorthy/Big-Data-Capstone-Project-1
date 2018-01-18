
hadoop fs -rm -r -f /bdprj1a/
hadoop jar proj1.jar bdprj1a /bdprjdata/h1b /bdprj1a/

echo -e "1 a) Is the number of petitions with Data Engineer job title increasing over time?\n\n"
hadoop fs -cat /bdprj1a/p*

