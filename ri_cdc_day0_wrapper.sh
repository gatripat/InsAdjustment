#!/bin/bash

#wrapper script for cdc day0 implementation of GCA
USER=`whoami`
kinit ${USER}@HADOOP.BARCLAYS.INTRANET -k -t ~/${USER}.keytab

# Export Application home directory
APP_HOME=/bigdata/projects/RI
CONF_DIR=$APP_HOME/config

prop_date=`cat $APP_HOME/lookup/prop_date`
SCRIPTNAME=`basename $0 .sh`



#source setup file
if [ -n "$APP_HOME" ]
then

PROP_FILE=$CONF_DIR/setAppl.sh
 . $PROP_FILE
    echo $PROP_FILE
    source $PROP_FILE
    cd $SCRIPTS_DIR
else
    echo "Application Direcotry not set"
    exit 1
fi

timestamp=`date +%s`
log_file=${SCRIPTNAME}
if [ -f $LOG_HOME/log4sh.sh ]
then
    . $LOG_HOME/log4sh.sh $LOG_HOME/$log_file
else
    echo "LOG file directory not exist"
fi

echo ${SCRIPTNAME}|logger_info

table_name=$1


if [ -n "$table_name" ]
then
    echo "Table name" | logger_info
else
   echo "Table name not entered" | logger_info
    exit 2
fi
echo $PROP_FILE

#executing cdc implementation for day0 ,passing config file as an argument and table_name
#hadoop jar  build/libs/ri_gca_cdc-1.0.0.jar com.barclays.RI.gca.cdc.RiGcaCdcDay0 param CUSTOMER_ACCOUNT_LINK

hadoop jar  $JAR_DIR/ri_gca_cdc-1.0.0.jar com.barclays.rid.gca.cdc.RiGcaCdcDay0 $PROP_FILE  $table_name $PROP_DATE 
is_valid=$?

 ROWID=`date +%s%N` 
#echo "put '${HBASE_SCOOP_LOG_RI}', '${ROWID}', '${HBASE_SCOOP_LOG_RI_CF}:${table_name}_cdc_last_exec', '${prop_date}'"|hbase shell 

#is_valid=$?
  if [ $is_valid -eq 0 ];
    then
echo "put '${HBASE_SCOOP_LOG_RI}', '${ROWID}', '${HBASE_SCOOP_LOG_RI_CF}:${table_name}_cdc_last_exec', '${prop_date}'"|hbase shell
 
 
		isValid=$?
		if [ $is_valid -ne 0 ]
			then
				echo "Hbase insert not executed successfully" | logger_info
				exit 2
			 else
				echo "Hbase insert  executed successfully " | logger_info
		fi
else
echo "CDC implementation not executed successfully"
fi

