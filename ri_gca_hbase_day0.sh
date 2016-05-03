#!/bin/bash

#kinit ${USER}@HADOOP.BARCLAYS.INTRANET -k -t ~/${USER}.keytab

# Export Application home directory
APP_HOME=/bigdata/projects/RI
CONF_DIR=$APP_HOME/config

# Source setup file - Environment variable
if [ -n "$APP_HOME" ]
then
    . $CONF_DIR/setEnv.sh
else
    echo "Application Direcotry not set" | logger_info
    exit 1
fi

# Source setup file - Application variable
if [ -n "$APP_HOME" ]
then
    . $CONF_DIR/setAppl.sh
    cd $SCRIPTS_DIR
else
    echo "Application Direcotry not set" | logger_info
    exit 1
fi

#Log file setup
if [ -f $LOG_HOME/log4sh.sh ]
then
    . $LOG_HOME/log4sh.sh $LOG_HOME/
else
    echo "LOG file directory not exist"
fi

# Getting Business date from the file(YYYY-MM-DD)
if [ -f $LOOKUP_DIR/prop_date ]
then
    BUSINESS_DATE=`cat $LOOKUP_DIR/prop_date`
    echo "Business date of execution: $BUSINESS_DATE" | logger_info
else
    echo "PROP file does not exist"
    exit 1
fi

yy=`echo ${BUSINESS_DATE}|cut -d '-' -f1`
month=`echo ${BUSINESS_DATE}|cut -d '-' -f2`
day=`echo ${BUSINESS_DATE}|cut -d '-' -f3`

year=`expr ${yy} - 3`

EXTRACTION_DATE_MINUS_3Y=`echo $year-$month-$day`

ROWID=`date +%s%N`
echo "put '${HBASE_SCOOP_LOG_RI}', '${ROWID}', '${HBASE_SCOOP_LOG_RI_CF}:ACCOUNT_ATTRIBUTES_RID_ext_last_exec', '${EXTRACTION_DATE_MINUS_3Y}'"|hbase shell
ROWID=`date +%s%N`
echo "put '${HBASE_SCOOP_LOG_RI}', '${ROWID}', '${HBASE_SCOOP_LOG_RI_CF}:CUSTOMER_ACCOUNT_LINK_RID_ext_last_exec', '${EXTRACTION_DATE_MINUS_3Y}'"|hbase shell
ROWID=`date +%s%N`
echo "put '${HBASE_SCOOP_LOG_RI}', '${ROWID}', '${HBASE_SCOOP_LOG_RI_CF}:DAILY_ACCOUNT_BALANCE_RID_ext_last_exec', '${EXTRACTION_DATE_MINUS_3Y}'"|hbase shell
ROWID=`date +%s%N`
echo "put '${HBASE_SCOOP_LOG_RI}', '${ROWID}', '${HBASE_SCOOP_LOG_RI_CF}:ORGANISATION_DEMOGRAPHY_RID_ext_last_exec', '${EXTRACTION_DATE_MINUS_3Y}'"|hbase shell
ROWID=`date +%s%N`
echo "put '${HBASE_SCOOP_LOG_RI}', '${ROWID}', '${HBASE_SCOOP_LOG_RI_CF}:REF_PRODUCT_HIERARCHY_RID_ext_last_exec', '${EXTRACTION_DATE_MINUS_3Y}'"|hbase shell
ROWID=`date +%s%N`
echo "put '${HBASE_SCOOP_LOG_RI}', '${ROWID}', '${HBASE_SCOOP_LOG_RI_CF}:ACCOUNT_HISTORICAL_DATA_RID_ext_last_exec', '${EXTRACTION_DATE_MINUS_3Y}'"|hbase shell


