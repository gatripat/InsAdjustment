package com.barclays.rid.gca.cdc;

import java.util.HashMap;
import java.util.Properties;
import org.apache.log4j.Logger;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Checkpoint;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.LeftJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.barclays.rid.gca.filter.DateFilter;
import com.barclays.rid.gca.utils.Utils;
public class RiGcaCdc{
	private static final Logger log = Logger.getLogger(RiGcaCdc.class);


	private static final String INDELIM = "INDELIM";
	private static final String OUTDELIM = "OUTDELIM";
	private static final String WORKFLOW = "Active+InActive";
	private static final String PKS = "_PK";
	private static final String END_DATE_FIELD = "_END_DATE_FIELD";
	private static final String PART =  "/part*";
	private static final String IPF="_IP_FIELDS";
	private static final String OPF="_OP_FIELDS";
	private static final String DELTA_INPATH="_DELTA_INPATH";
	private static final String INACTIVE_INPATH="_INACTIVE_INPATH";
	private static final String ACTIVE_INPATH="_ACTIVE_INPATH";


	public static void main(String[] args) {
		HashMap<String, String> parameter = new HashMap<String, String>();
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, RiGcaCdc.class);
		AppProps.addApplicationTag(properties, "RI_GCA_CDC");


		Utils paramP=new Utils();
		String inputTable="";
		String propDate="";
		String deltaSource="";
		String activeSource="";
		String inactiveSource="";
		String inDelim="";
		String outdDelim="";
		String endDateField = "";
		String activeOutpath = "";
		String inactiveOutpath = "";
		String deltaInpath= "";
		String activeInpath= "";   
		String lastExecDate="";
		if(paramP.checkNull(args[0])&&paramP.checkNull(args[1])&&paramP.checkNull(args[2])&&paramP.checkNull(args[3]))	
		{
			parameter = paramP.paramParser(args[0]);
			inputTable = args[1].trim();
			propDate=args[2].trim();
			lastExecDate=args[3].trim();
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
			deltaInpath= deltaSource+propDate+PART;
			activeInpath= activeSource+lastExecDate+PART;        		
			activeOutpath = activeSource+propDate;
			inactiveOutpath =  inactiveSource+propDate;
			endDateField  = parameter.get(inputTable + END_DATE_FIELD);

		}else
		{
			log.fatal("Assign parameter value in property file");
			
		}	

		String  PK=parameter.get(inputTable + PKS);
		Fields  inputFields  = new Fields( parameter.get(inputTable+IPF).split(",",-1));
		Fields  outputFields  = new Fields( parameter.get(inputTable+OPF).split(",",-1));


		Fields pk = new Fields(PK.split(",",-1));
		String stringPk1 = "";
		String stringPk2 = "";
		for (String key : PK.split(",", -1)) {
			stringPk1 = stringPk1 + "," + key + "_1";
			stringPk2 = stringPk2 + "," + key + "_2";

		}

		Fields pk1 = new Fields(stringPk1.substring(1, stringPk1.length()).split(",",-1));
		Fields pk2 = new Fields(stringPk2.substring(1, stringPk2.length()).split(",",-1));
		Fields gKey = new Fields("size");

		Pipe delta = new Pipe("delta");
		Pipe activeNow = new Pipe("active");
		Pipe activeDelta = new Pipe("merged");

		Pipe g1 = new Pipe("group");
		activeDelta = new Merge(delta, activeNow);
		activeDelta = new Rename(activeDelta, pk, pk2);
		Pipe UPipe = new Unique(activeDelta, Fields.ALL);

		g1 = new GroupBy(activeDelta, pk2, true);
		g1 = new Every(g1, Fields.ALL, new Count(gKey), Fields.ALL);
		g1 = new Rename(g1, pk2, pk1);

		final Checkpoint userPipeCheckpoint = new Checkpoint("checkpoint", g1);

Pipe groupedFull = new CoGroup(UPipe, pk2, userPipeCheckpoint, pk1, new LeftJoin());
		
		Checkpoint groupedFullTuple = new Checkpoint("checkpointCoGrp", groupedFull);


		ExpressionFilter filterU = new ExpressionFilter("size == 1", Integer.TYPE);
		Pipe updateRecordPipe = new Each(groupedFullTuple, new Fields("size"), filterU);

		DateFilter updateFilterClosed =new DateFilter(">", propDate);

		Pipe updateRecordClosedPipe = new Each(updateRecordPipe, new Fields(endDateField), updateFilterClosed);

		ExpressionFilter insertFilterID = new ExpressionFilter("size > 1", Integer.TYPE);
		Pipe insertRecordPipe = new Each(groupedFullTuple, new Fields("size"), insertFilterID);
		Checkpoint chkInsertRecordPipe= new Checkpoint("checkpointinsertRecordPipe", insertRecordPipe);

		DateFilter insertFilterClosed =new DateFilter(">",propDate);

		Pipe insertInactiveRecords = new Each(chkInsertRecordPipe, new Fields(endDateField), insertFilterClosed);

		DateFilter insertFilterOpen =new DateFilter("<=", propDate);

		Pipe activeRecords = new Each(chkInsertRecordPipe, new Fields(endDateField), insertFilterOpen);


		Checkpoint inactive = new Checkpoint("checkpointActive", insertInactiveRecords);
		//      Pipe activeRecords =insertActiveRecords ;
		Pipe inactiveRecords = new Merge(updateRecordClosedPipe, inactive);
		Hadoop2MR1FlowConnector flowConnector1 = new Hadoop2MR1FlowConnector(properties);

		Hfs deltaTap = new Hfs(new TextDelimited(inputFields, false, inDelim), deltaInpath);
		Hfs activeTap = new Hfs(new TextDelimited(inputFields, false, outdDelim), activeInpath);
		Hfs sinkInActive = new Hfs(new TextDelimited(outputFields, false, outdDelim), inactiveOutpath);
		Hfs sinkActive = new Hfs(new TextDelimited(outputFields, false, outdDelim), activeOutpath);

		FlowDef flowDef1 = new FlowDef().setName(WORKFLOW).addSource(delta, deltaTap)
				.addSource(activeNow, activeTap).addSink(activeRecords, sinkActive).addTail(activeRecords)
				.addSink(inactiveRecords, sinkInActive).addTail(inactiveRecords);

		Flow<?> flow1 = flowConnector1.connect(flowDef1);
		flow1.complete();

	}
}
