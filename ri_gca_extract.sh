#!/bin/bash
# Script Name - ri_GCA_EXTRACT.sh
# Purpose - Used for extraction of table in RI project


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


# Parsing command line arguments
usage() { echo "Usage: $0 [-l <BASE|DELTA>] [-t <table name>] [-t <table name 2...(optional)>] [-m <number of mapper(optional)>]" 1>&2; exit 1; }
while getopts ":l:t:m:" o; do
    case "${o}" in
        l)
            MODE=${OPTARG}
            if [ ${MODE} != "BASE" ] && [ ${MODE} !=  "DELTA" ]; then
                usage
            fi
            ;;
        t)
            aTABLE+=(${OPTARG})
            ;;
        m)
            MAPPER=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${MODE}" ] || [ -z "${aTABLE}" ]; then
    usage
fi

if [ -z "${MAPPER}" ]; then
    echo "Mapper value is not supplied using default value ${NUMMAPPER} " | logger_info
    MAPPER=${NUMMAPPER}
fi

# Getting count of incoming TABLE
count=${#aTABLE[@]}

# Exit with error if unset environments are found
set -u

SCRIPTNAME=`basename $0 .sh`

# Creating return value
iRet=0

# Getting Business date from the file(YYYY-MM-DD)
if [ -f $LOOKUP_DIR/prop_date ]
then 
    PROP_DATE=`cat $LOOKUP_DIR/prop_date`
    echo "PROP date of execution: $PROP_DATE" | logger_info
else 
    echo "PROP file does not exist"
    exit 1
fi

# Spliting Business date into dd, mm, yy
yy=`echo ${PROP_DATE}|cut -d '-' -f1`
mm=`echo ${PROP_DATE}|cut -d '-' -f2`
dd=`echo ${PROP_DATE}|cut -d '-' -f3`

#Calculating dates for Monthly table
ENDDATE_36=`date -d "${PROP_DATE}  -${dd} days" +%Y-%m-%d`
dd=`expr ${dd} - 1`
STARTDATE_36=`date -d "${PROP_DATE}  -${dd} days -36 month" +%Y-%m-%d`

#Password Decrypting
#DBPASSWD_T=`java -jar ${COM_BIN}/security.jar getPassword ${DBPASSWD_T}|tail -1`
#iRet=$?
#if [ $iRet -ne 0  ]
#then
#    echo "Failed to decrypt password "| logger_info
#    exit $iRet
#fi


for (( i=0; i<${count}; i++ ))
do
    TABLE=${aTABLE[$i]^^}
    echo "Processing table : $TABLE "

    if [ ${TABLE} != "ACCOUNT_ATTRIBUTES_RID" ] && [ ${TABLE} != "CUSTOMER_ACCOUNT_LINK_RID" ] && [ ${TABLE} != "DAILY_ACCOUNT_BALANCE_RID" ] && [ ${TABLE} != "ORGANISATION_DEMOGRAPHY_RID" ] && [ ${TABLE} != "REF_PRODUCT_HIERARCHY_RID" ] && [ ${TABLE} != "ACCOUNT_HISTORICAL_DATA_RID" ]; 
    then
    echo "Invalid Table name supplied ${TABLE}" | logger_info
        usage
    fi
 
    echo "Mapper value is not supplied using default value ${NUMMAPPER} " | logger_info
	
    HDFS_DIR=${TABLE}_HDP
    

    # Business date is last month's end date in case of table ACCOUNT_HISTORICAL_DATA_RID
    
    if [ ${TABLE} == "ACCOUNT_HISTORICAL_DATA_RID" ]
    then
        BUSINESS_DATE=${ENDDATE_36}
    else
        BUSINESS_DATE=${PROP_DATE}
    fi

    echo "PROP_DATE - ${PROP_DATE}"| logger_info
    echo "BUSINESS DATE - ${BUSINESS_DATE}"| logger_info
    echo "STARTDATE_36 - ${STARTDATE_36}"| logger_info
    echo "ENDDATE_36 - ${ENDDATE_36}"| logger_info



    # Checking if todays job already executed
    if hadoop fs -test -e ${HDFS_GCA_EXTRACT}/${HDFS_DIR}/${BUSINESS_DATE};
    then
        if [ ${TABLE} == "ACCOUNT_HISTORICAL_DATA_RID" ]
        then
            echo "Job already executed for last 12 month period for table ${TABLE}" | logger_info
        else
            echo "Job already executed for this day ${BUSINESS_DATE} for table ${TABLE}" | logger_info
            continue
        fi
    else
        #LAST_EXT_DATE=`eval "hadoop jar ${JAR_DIR}/cbp.jar source_id ${HBASE_SCOOP_LOG_RI} ${HBASE_SCOOP_LOG_RI_CF} ${TABLE}" | tail -1`

        echo "scan '${HBASE_SCOOP_LOG_RI}'"|hbase shell|grep "column=sources:${TABLE}_ext_last_exec" >${LOG_HOME}/${SCRIPTNAME}_hbase_insert.log
        iCount=0;
        iCount=`grep -c "column=sources:${TABLE}_ext_last_exec" $LOG_HOME/${SCRIPTNAME}_hbase_insert.log`

        if [ $iCount -gt 0 ]
        then
            echo "Data get from last scan operation"| logger_info
        else
            echo "Failed to fetch last run date in HBASE for table ${TABLE} "| logger_info
            exit 1
        fi

        LAST_EXT_DATE=`grep "column=sources:${TABLE}_ext_last_exec" ${LOG_HOME}/${SCRIPTNAME}_hbase_insert.log|tail -1|cut -d ' ' -f5|cut -d '=' -f2`

        if [ -z "${LAST_EXT_DATE}" ]
        then
            echo "Failed to fetch date from HBASE, First execute BASE load"| logger_info
            exit 1
        else
            echo " Last execution date fetched from HBASE :  ${LAST_EXT_DATE}" | logger_info
        fi
    fi
    
    case "$TABLE" in

    "ACCOUNT_ATTRIBUTES_RID")
	
    if [ ${MODE} == "DELTA" ]
    then
	SQOOP_QUERY="select to_char(LAST_UPDATE_DATE,'YYYY-MM-DD'),ACCOUNT_ID,trim(COALESCE(ACCOUNT_NAME,'null')) as ACCOUNT_NAME,trim(COALESCE(ACCOUNT_NUMBER,'null')) as ACCOUNT_NUMBER ,to_char(EFFECTIVE_START_DATE,'YYYY-MM-DD'),to_char(EFFECTIVE_END_DATE,'YYYY-MM-DD'),ACCOUNT_SOURCE,trim(COALESCE(ACCOUNT_SORT_CODE,'null')) as ACCOUNT_SORT_CODE from ACCOUNT_ATTRIBUTES_RID WHERE LAST_UPDATE_DATE > to_date('${LAST_EXT_DATE}','YYYY-MM-DD') AND LAST_UPDATE_DATE <= to_date('${BUSINESS_DATE}','YYYY-MM-DD') AND \$CONDITIONS"
    else
       SQOOP_QUERY="select to_char(LAST_UPDATE_DATE,'YYYY-MM-DD'),ACCOUNT_ID,trim(COALESCE(ACCOUNT_NAME,'null')) as ACCOUNT_NAME,trim(COALESCE(ACCOUNT_NUMBER,'null')) as ACCOUNT_NUMBER ,to_char(EFFECTIVE_START_DATE,'YYYY-MM-DD'),to_char(EFFECTIVE_END_DATE,'YYYY-MM-DD'),ACCOUNT_SOURCE,trim(COALESCE(ACCOUNT_SORT_CODE,'null')) as ACCOUNT_SORT_CODE from ACCOUNT_ATTRIBUTES_RID WHERE  \$CONDITIONS"
    fi

    ;; 
    
	
    "CUSTOMER_ACCOUNT_LINK_RID")
	
    if [ ${MODE} == "DELTA" ]
    then
        SQOOP_QUERY="select to_char(LAST_UPDATE_DATE,'YYYY-MM-DD'),CUSTOMER_ID,ACCOUNT_ID,to_char(EFFECTIVE_START_DATE,'YYYY-MM-DD'),to_char(EFFECTIVE_END_DATE,'YYYY-MM-DD'),CURRENCY_CODE,CUSTOMER_SOURCE,ACCOUNT_SOURCE,to_char(MATURITY_DATE,'YYYY-MM-DD') from CUSTOMER_ACCOUNT_LINK_RID WHERE LAST_UPDATE_DATE > to_date('${LAST_EXT_DATE}','YYYY-MM-DD') AND LAST_UPDATE_DATE <= to_date('${BUSINESS_DATE}','YYYY-MM-DD') AND \$CONDITIONS"
    else
        SQOOP_QUERY="select to_char(LAST_UPDATE_DATE,'YYYY-MM-DD'),CUSTOMER_ID,ACCOUNT_ID,to_char(EFFECTIVE_START_DATE,'YYYY-MM-DD'),to_char(EFFECTIVE_END_DATE,'YYYY-MM-DD'),CURRENCY_CODE,CUSTOMER_SOURCE,ACCOUNT_SOURCE,to_char(MATURITY_DATE,'YYYY-MM-DD') from CUSTOMER_ACCOUNT_LINK_RID WHERE \$CONDITIONS"
    fi
    
    ;;

    "DAILY_ACCOUNT_BALANCE_RID")
        SQOOP_QUERY="select to_char(LAST_UPDATE_DATE,'YYYY-MM-DD'),ACCOUNT_ID,to_char(SUMMARY_DATE,'YYYY-MM-DD'),BALANCE_AMT,PRODUCT_IDENTIFIER,CURRENCY_CODE,COALESCE(LIMIT_AMT,0) as LIMIT_AMT,trim(COALESCE(PRODUCT_RATE,'null')) as PRODUCT_RATE,ACCOUNT_SOURCE from DAILY_ACCOUNT_BALANCE_RID where LAST_UPDATE_DATE > to_date('${LAST_EXT_DATE}','YYYY-MM-DD') AND LAST_UPDATE_DATE <= to_date('${BUSINESS_DATE}','YYYY-MM-DD') AND \$CONDITIONS"
    ;;

    "ORGANISATION_DEMOGRAPHY_RID")
    if [ ${MODE} == "DELTA" ]
    then
        SQOOP_QUERY="select to_char(LAST_UPDATE_DATE,'YYYY-MM-DD'),CUSTOMER_ID,CUSTOMER_SOURCE,CUSTOMER_NAME,BIC_CODE,to_char(EFFECTIVE_START_DATE,'YYYY-MM-DD'),to_char(EFFECTIVE_END_DATE,'YYYY-MM-DD') from ORGANISATION_DEMOGRAPHY_RID WHERE LAST_UPDATE_DATE > to_date('${LAST_EXT_DATE}','YYYY-MM-DD') AND LAST_UPDATE_DATE <= to_date('${BUSINESS_DATE}','YYYY-MM-DD') AND \$CONDITIONS"
    else
        SQOOP_QUERY="select to_char(LAST_UPDATE_DATE,'YYYY-MM-DD'),CUSTOMER_ID,CUSTOMER_SOURCE,CUSTOMER_NAME,BIC_CODE,to_char(EFFECTIVE_START_DATE,'YYYY-MM-DD'),to_char(EFFECTIVE_END_DATE,'YYYY-MM-DD') from ORGANISATION_DEMOGRAPHY_RID WHERE \$CONDITIONS"
    fi

    ;;

    "REF_PRODUCT_HIERARCHY_RID")
        SQOOP_QUERY="select PRODUCT_IDENTIFIER,PRODUCT_NAME,PRODUCT_LEVEL_1,PRODUCT_LEVEL_2,PRODUCT_LEVEL_3 from REF_PRODUCT_HIERARCHY_RID WHERE \$CONDITIONS"
    ;;

    "ACCOUNT_HISTORICAL_DATA_RID")
        SQOOP_QUERY="select to_char(LAST_UPDATE_DATE,'YYYY-MM-DD'),CUSTOMER_ID,CUSTOMER_SOURCE,ACCOUNT_ID,ACCOUNT_SOURCE,PRODUCT_IDENTIFIER,to_char(SUMMARY_MONTH,'YYYY-MM-DD'),AVERAGE_BALANCE_AMT,WORST_BALANCE_AMT,OD_DAYS from ACCOUNT_HISTORICAL_DATA_RID WHERE SUMMARY_MONTH >= to_date('${STARTDATE_36}','YYYY-MM-DD') AND SUMMARY_MONTH <= to_date('${ENDDATE_36}','YYYY-MM-DD') AND \$CONDITIONS"
    ;;

    esac

    echo "SQOOP_QUERY : ${SQOOP_QUERY}" | logger_info

    #SQOOP Import
    #sqoop import  --connect $DB_CONNECTION  --username $DBUSERID --password $DBPASSWD --target-dir ${HDFS_GCA_EXTRACT}/${HDFS_DIR}/${BUSINESS_DATE}  --as-textfile --fields-terminated-by '|'  -m "$MAPPER" --query "${SQOOP_QUERY}"
    
    sqoop import  --connect $DB_CONNECTION_T  --username $DBUSERID_T --password $DBPASSWD_T --target-dir ${HDFS_GCA_EXTRACT}/${HDFS_DIR}/${BUSINESS_DATE}  --as-textfile --fields-terminated-by '|'  -m "$MAPPER" --query "${SQOOP_QUERY}"

    iRet=$?

    if [ $iRet -ne 0 ]
    then
        echo "Error in SQOOP extraction is not OK : $iRet " | logger_info
        hadoop fs -rm -r ${HDFS_GCA_EXTRACT}/${HDFS_DIR}/${BUSINESS_DATE}
        exit $iRet
    fi

    if hadoop fs -test -e ${HDFS_GCA_EXTRACT}/${HDFS_DIR}/${BUSINESS_DATE}/_SUCCESS;
    then
        echo "*****SQOOP Success File exists*****" | logger_info
       
        # Inserting last run date in HBASE 
        #hadoop jar ${JAR_DIR}/cbp.jar put ${HBASE_SCOOP_LOG_RI} ${HBASE_SCOOP_LOG_RI_CF} ${BUSINESS_DATE} ${TABLE}_ext_last_exec ${BUSINESS_DATE}

        if [ ${TABLE} = "ACCOUNT_ATTRIBUTES_RID" ] || [ ${TABLE} = "CUSTOMER_ACCOUNT_LINK_RID" ] || [ ${TABLE} = "DAILY_ACCOUNT_BALANCE_RID" ] || [ ${TABLE} = "ORGANISATION_DEMOGRAPHY_RID" ] 
        then
            LAST_EXTRACTION_DATE=`hadoop fs -cat ${HDFS_GCA_EXTRACT}/${HDFS_DIR}/${BUSINESS_DATE}/part* | cut -d '|' -f1 | sort -r | head -1`
	else
            LAST_EXTRACTION_DATE=${BUSINESS_DATE}
        fi

        #Case when no records fetched for the mentioned duration 
        if [ -z "$LAST_EXTRACTION_DATE" ]
        then 
            echo "Zero row fetched, updaing last extraction date as Business date" | logger_info
            LAST_EXTRACTION_DATE=${LAST_EXT_DATE}
        fi

        ROWID=`date +%s%N`

        echo "put '${HBASE_SCOOP_LOG_RI}', '${ROWID}', '${HBASE_SCOOP_LOG_RI_CF}:${TABLE}_ext_last_exec', '${LAST_EXTRACTION_DATE}'"|hbase shell > $LOG_HOME/${SCRIPTNAME}_hbase_insert.log 

        iCount=`grep -c "0 row(s) in" $LOG_HOME/${SCRIPTNAME}_hbase_insert.log`
   
        if [ $iCount -eq 1 ]
        then
            echo "Date inserted in HBASE for table ${TABLE} "| logger_info
        else
            echo "Failed to insert last run date in HBASE for table ${TABLE} "| logger_info
            hadoop fs -rm -r ${HDFS_GCA_EXTRACT}/${HDFS_DIR}/${BUSINESS_DATE}
            exit 1
        fi

        #iRet=$?
        #if [ $iRet -ne 0 ]
        #then
        #     echo "Failed to insert last run date in HBASE : $iRet"
        #     exit $iRet
        #fi
        #load flag is not required for this script. Just in line with cbp.jar
        #hadoop jar ${JAR_DIR}/cbp.jar put ${HBASE_SCOOP_LOG_RI} ${HBASE_SCOOP_LOG_RI_CF} ${BUSINESS_DATE} ${TABLE}_load_flag 1
	 #iRet=$?
        #if [ $iRet -ne 0 ]
        #then
        #     echo "Failed to insert load flag in HBASE : $iRet"
        #     exit $iRet
        #fi

        #Merge flag is not required for this script. Just in line with cbp.jar
        #hadoop jar ${JAR_DIR}/cbp.jar put ${HBASE_SCOOP_LOG_RI} ${HBASE_SCOOP_LOG_RI_CF} ${BUSINESS_DATE} ${TABLE}_merge_flag 1
        #iRet=$?
        #if [ $iRet -ne 0 ]
        #then
        #     echo "Failed to insert  run date in HBASE : $iRet"
        #     exit $iRet
        #fi

    else
        echo "Error in SQOOP Extraction, SUCCESS file not found"
        echo "Removing output directory"
        hadoop fs -rm -r ${HDFS_GCA_EXTRACT}/${HDFS_DIR}/${BUSINESS_DATE}
        exit 1
    fi

    echo "Table $TABLE processing get completed with return code : $iRet "
done
