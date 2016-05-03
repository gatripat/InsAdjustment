package com.barclays.rid.gca.cdc;

import java.util.HashMap;
import java.util.Properties;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.barclays.rid.gca.filter.DateFilter;
import com.barclays.rid.gca.utils.Utils;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import org.apache.log4j.Logger;

public class RiGcaCdcDay0{
	private static final Logger log = Logger.getLogger(RiGcaCdcDay0.class);

	private static final String INDELIM = "INDELIM";
	private static final String OUTDELIM = "OUTDELIM";
	private static final String WORKFLOW = "Active+InActive";

	private static final String END_DATE_FIELD = "_END_DATE_FIELD";
	private static final String PART =  "/part*";
	private static final String IPF="_IP_FIELDS";
	private static final String DELTA_INPATH="_DELTA_INPATH";
	private static final String INACTIVE_INPATH="_INACTIVE_INPATH";
	private static final String ACTIVE_INPATH="_ACTIVE_INPATH";


	public static void main(String[] args) {
		HashMap<String, String> parameter = new HashMap<String, String>();
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, RiGcaCdcDay0.class);
		AppProps.addApplicationTag(properties, "RI_GCA_CDC");

		Utils paramP=new Utils();
		String deltaSource="";
		String activeSource="";
		String inactiveSource="";
		String inDelim="";
		String outdDelim="";
		String endDateField = "";
		String inpathDay0= "";		
		String activeOutpath = "";
		String inactiveOutpath = "";
		Fields inputFields =null ;
		String inputTable="";
		String propDate="";

		if(paramP.checkNull(args[0])&&paramP.checkNull(args[1])&&paramP.checkNull(args[2]))	
		{
			parameter = paramP.paramParser(args[0]);
			inputTable = args[1].trim();
			propDate=args[2].trim();
			log.info(parameter);
		}
		else
		{
			log.fatal("Provide all command line arguments");
			
		}
		if(paramP.checkNull(parameter.get(inputTable+DELTA_INPATH))&& paramP.checkNull(parameter.get(inputTable+ACTIVE_INPATH))
				&&paramP.checkNull(parameter.get(inputTable+INACTIVE_INPATH))&&
				paramP.checkNull(parameter.get(INDELIM))&&paramP.checkNull(parameter.get(OUTDELIM))&&
				paramP.checkNull(parameter.get(inputTable + END_DATE_FIELD)))
		{

			deltaSource=parameter.get(inputTable+DELTA_INPATH);
			activeSource=parameter.get(inputTable+ACTIVE_INPATH);
			inactiveSource=parameter.get(inputTable+INACTIVE_INPATH);
			inDelim=parameter.get(INDELIM);
			outdDelim=parameter.get(OUTDELIM);
			endDateField = parameter.get(inputTable + END_DATE_FIELD);
			inpathDay0= deltaSource+propDate+PART;			
			activeOutpath = activeSource+propDate;
			inactiveOutpath =  inactiveSource+propDate;

			log.info("INPATH"+inpathDay0);		
			log.info("ACTIVE OUTPATH"+activeOutpath);
			log.info("INACTIVE OUTPATH"+inactiveOutpath);
			log.info("DELIM"+inDelim);
			inputFields  = new Fields( parameter.get(inputTable+IPF).split(",",-1));

		}
		{
			log.fatal("Assign parameter value in property file");
		}
		Pipe input1 = new Pipe("input1");
		Pipe input2 = new Pipe("input2");

		Pipe inactiveRecordsPipe=new Pipe("inactive");
		Pipe activeRecordsPipe=new Pipe("active");

		log.info("prop_date"+propDate);
		log.info("end_date"+endDateField);

		DateFilter inactiveRecordsFilter =new DateFilter(">", propDate);

		inactiveRecordsPipe = new Each(input1, new Fields(endDateField), inactiveRecordsFilter);

		DateFilter activeRecordsFilter=new DateFilter("<=", propDate);

		activeRecordsPipe = new Each(input2, new Fields(endDateField), activeRecordsFilter);

		Hfs inputTap = new Hfs(new TextDelimited(inputFields, false, inDelim), inpathDay0);		
		Hfs sinkInActive = new Hfs(new TextDelimited(inputFields, false, outdDelim), inactiveOutpath);
		Hfs sinkActive = new Hfs(new TextDelimited(inputFields, false, outdDelim), activeOutpath);


		FlowDef flowDef1 = new FlowDef().setName(WORKFLOW).addSource(input1, inputTap).addSource(input2, inputTap)
				.addSink(activeRecordsPipe, sinkActive).addTail(activeRecordsPipe)
				.addSink(inactiveRecordsPipe, sinkInActive).addTail(inactiveRecordsPipe);

		Hadoop2MR1FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

		Flow<?> flow1 = flowConnector.connect(flowDef1);
		flow1.complete();
	}
}

