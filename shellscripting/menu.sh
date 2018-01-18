#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<H1B CASE STUDY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 1) ${MENU} Is the number of petitions with Data Engineer job title increasing over time?${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 2) ${MENU} Find top 5 job titles who are having highest avg growth in applications. ${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 3) ${MENU} Which part of the US has the most Data Engineer jobs for each year? ${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 4) ${MENU} find top 5 locations in the US who have got certified visa for each year.${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 5) ${MENU} Which industry has the most number of Data Scientist positions?${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 6) ${MENU} Which top 5 employers file the most petitions each year? ${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 7) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year?(FOR ALL 						   APPLICANTS)${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 8) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year?(FOR CERTIFIED 					           APPLICANTS)${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 9) ${MENU} Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 10) ${MENU} Create a bar graph to depict the number of applications for each year${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 11) ${MENU}Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 12) ${MENU} Which are employers who have the highest success rate in petitions more than 70% in petitions and total petions filed more than 1000?${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 13) ${MENU} Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000? ${NORMAL}"
    echo -e "${MENU}>>${NUMBER} 14) ${MENU}Export result for option no 12 to MySQL database.${NORMAL}"
    echo -e "${MENU}<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}

clear
show_menu
	while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1) clear;
	start-all.sh

        echo -e  "1 a) Is the number of petitions with Data Engineer job title increasing over time?";
		temp=$(pwd)
		
		bash /home/hduser/Desktop/project/mapreduce/shells/1a.sh
		cd $temp
        show_menu;
        ;;
        2) clear;
	stop-all.sh

        exho -e "1 b) Find top 5 job titles who are having highest growth in applications. ";
        
	   pig -x local /home/hduser/Desktop/project/pig/1b.pig

        show_menu;
        ;;  
        3) clear;
	start-all.sh

        echo -e "2 a) Which part of the US has the most Data Engineer jobs for each year?";
	
		 echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		 read yr      
		hive -e "select year,worksite,count(*)as total from bdprj.h1b_final where job_title='DATA ENGINEER' and year=2011 and case_status='CERTIFIED' GROUP BY year,worksite order by total desc limit 1;"
        show_menu;
        ;;
	    4) clear;
	start-all.sh

        echo -e "2 b) find top 5 locations in the US who have got certified visa for each year.";
        echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read yr
	    hive -e " select year,count(*)as total ,worksite from bdprj.h1b_final where year=$yr and case_status='CERTIFIED' group by  year,worksite order by total desc limit 5;" 
        show_menu;
        ;;  
	    5) clear;
start-all.sh

        echo -e "3) Which industry has the most number of Data Scientist positions?";
                
		hive -e "select soc_name,job_title,count(*)as total from bdprj.h1b_final where case_status='CERTIFIED' and job_title='DATA SCIENTIST' group by soc_name,job_title order by total desc limit 1;"
        show_menu;
        ;;
        6) clear;
start-all.sh

        echo -e "4)Which top 5 employers file the most petitions each year?";
		echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read yr
		hive -e "select year,employer_name,count(*)as total from bdprj.h1b_final where year=$yr group by year,employer_name order by total desc limit 5;"
        show_menu;
        ;;
        7) clear;
start-all.sh

        echo -e "5a) Find the most popular top 10 job positions for H1B visa applications for each year? for all the applications";
	    temp=$(pwd)
		
		bash /home/hduser/Desktop/project/mapreduce/shells/5a.sh
		cd $temp
        show_menu;
        ;;
        8) clear;
start-all.sh

        echo -e "5b) Find the most popular top 10 job positions for H1B visa applications for each year? for certified applications";
	    temp=$(pwd)
		bash /home/hduser/Desktop/project/mapreduce/shells/5b.sh
		cd $temp
        show_menu;
        ;;
        9) clear;
       	stop-all.sh

	echo -e "6) Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.";
		pig -x local /home/hduser/Desktop/project/pig/6.pig
		bash /home/hduser/Desktop/project/pig/graphs.sh
        show_menu;
        ;;
		10) clear;
		start-all.sh

		
        echo -e "7) Create a bar graph to depict the number of applications for each year";
		temp=$(pwd)
		
		bash /home/hduser/Desktop/project/mapreduce/shells/7.sh
		cd $temp
                show_menu;
        ;;
		11) clear;
start-all.sh

        echo -e "8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order";
		echo -e "Enter the year(2011,2012,2013,2014,2015,2016)"
		read yr
		echo -e "Enter the choice Full time/ Part time with capital letters(Y/N)"
		read ftp
        hive -e "select year,job_title,full_time_position,avg(prevailing_wage)as average from bdprj.h1b_final where year=$yr and full_time_position='$ftp' and (case_status='CERTIFIED' or case_status='CERTIFIED-WITHDRAWN') group by year,job_title,full_time_position order by average desc; "
        show_menu;
        ;;
		12) clear;
		stop-all.sh

		echo -e "9) Which are   employers who have the highest success rate in petitions more than 70% in petitions and total petions filed more than 1000?"
		
		pig -x local /home/hduser/Desktop/project/pig/9.pig
        show_menu;
        ;;
		13) clear;
		stop-all.sh

		echo -e "10) Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000?"
		
		rm -r -f /home/hduser/Desktop/pig10sol
		pig -x local /home/hduser/Desktop/project/pig/10.pig
		
        show_menu;
        ;;
		14) clear;
start-all.sh

		echo -e "11) Export result for question no 10 to MySql database."
		start-all.sh
		sleep 10;
		bash /home/hduser/Desktop/project/sqoop/11.sh
        show_menu;
        ;;
		\n) exit;   
		;;
        *) clear;
        
        show_menu;
        ;;
    esac
fi
done
