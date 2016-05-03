#!/bin/bash

#kinit ${USER}@HADOOP.BARCLAYS.INTRANET -k -t ~/${USER}.keytab

#Creating unix directory structure
#Checking project parent directory

DATESTAMP=`date '+%Y%m%d %H:%M:%S.%3N'`
APP_HOME=/bigdata/projects/RI
LOGFILE=/tmp/ri_directory_setup.log

echo "Script executed at - $DATESTAMP" | tee -a $LOGFILE
echo "LOG file Name - $LOGFILE" | tee -a $LOGFILE

if [ -d "${APP_HOME}" ]
then
    echo "Directory ${APP_HOME}" | tee -a $LOGFILE
else
    mkdir -p ${APP_HOME}
    chmod -R 770 ${APP_HOME}
fi


#Checking and creating sub directory inside the project directory
for var in tools outbound inbound bin lookup logs config scripts
do
    if [ -d "${APP_HOME}/${var}" ]
    then
        echo "Directory exists - ${APP_HOME}/${var}" | tee -a $LOGFILE
    else
        echo "Creating directory - ${APP_HOME}/${var} " | tee -a $LOGFILE
        mkdir -p "${APP_HOME}/${var}"
        chmod -R 770 "${APP_HOME}/${var}"
    fi
done

#Checking and creating sub directory inside scripts
for var in aggregate extract setup preaggreg
do
    if [ -d "${APP_HOME}/scripts/${var}" ]
    then
        echo "Directory exists - ${APP_HOME}/scripts/${var}" | tee -a $LOGFILE
    else
        echo "Creating directory - ${APP_HOME}/scripts/${var} " | tee -a $LOGFILE
        mkdir -p "${APP_HOME}/${var}"
        chmod -R 770 "${APP_HOME}/${var}"
    fi
done


# Creating HDFS directory structure - DirectoryFile contains list of directory that needs to be created.
for var in `cat DirectoryFile`
do
    if hadoop fs -test -e ${var}
    then
        echo "HDFS Directory exists - ${var}" | tee -a $LOGFILE
    else
        echo "Creating HDFS dir - ${var} " | tee -a $LOGFILE
        hadoop fs -mkdir -p ${var}
    fi
done
