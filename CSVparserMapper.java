package com.barclays.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

/* author @gatripat
 * 
 */
public class CSVparserMapper extends Mapper<LongWritable, Text, Text, Text> {
	Logger log = Logger.getLogger(CSVparserMapper.class);
	private Text word = new Text();
	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	String[] token;
	String id = "";
	String startPeriod = "";
	String endPeriod = "";
	String desc = "";
	String lowerBoundary = "1988";
	String upperBoundary = "2020";
	Integer boundaryPointer = 0;
	static String annual = "A";
	static String month = "M";
	static String quarter = "Q";
	static String DELIM = ",";
	String region = "";
	HashMap<String, String> data = new HashMap<String, String>();
	SimpleDateFormat yearPattern = new SimpleDateFormat("yyyy");
	SimpleDateFormat monthPattern = new SimpleDateFormat("yyyy-MMM");
	// SimpleDateFormat quarterPattern = new SimpleDateFormat("yyyy-MMM");
	Integer noOfCells = 0;

	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		 multipleOutputs = new MultipleOutputs(context);
		lowerBoundary = conf.get("LOWER_DATE");
		upperBoundary = conf.get("HIGHER_DATE");

	}

	private Integer getNoOfQuartersCell(String startYearQuartrComplex, String endYearQuartrComplex) {
		int count = 0;
		
		Integer startYear = Integer.parseInt(startYearQuartrComplex.split("-", -1)[0]);
		String startQuatr = startYearQuartrComplex.split("-", -1)[1];

		Integer endYear = Integer.parseInt(endYearQuartrComplex.split("-", -1)[0]);
		String endQuatr = endYearQuartrComplex.split("-", -1)[1];

		Integer startQuarterNumber = Integer.parseInt(startQuatr.trim().substring(startQuatr.length() - 1));
		Integer endQuarterNumber = Integer.parseInt(endQuatr.trim().substring(endQuatr.length() - 1));
		int yrcount = (startYear == endYear) ? (endYear - startYear) * 4 : (endYear - startYear - 1) * 4;
		count = yrcount + (((endQuarterNumber) + (4 - startQuarterNumber)+1));
		return count;

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		token = value.toString().split(",", -1);
		NullWritable outKey = NullWritable.get();
		id = token[0];

		startPeriod = token[1];			
		endPeriod = token[2];
		desc = token[3];
		region = desc.split(":", -1)[0];
		String keyy = "";
		try {
			Calendar calendarLower = Calendar.getInstance();
			calendarLower.setTime(yearPattern.parse(lowerBoundary));
			
			Calendar calendarLower1 = Calendar.getInstance();
			calendarLower1.setTime(yearPattern.parse(lowerBoundary));

			Calendar calendarDataLower = Calendar.getInstance();
			calendarDataLower.setTime(yearPattern.parse(startPeriod));
			Calendar calendarDataHigher = Calendar.getInstance();
			calendarDataHigher.setTime(yearPattern.parse(endPeriod));
			Calendar calendarHigher = Calendar.getInstance();
			calendarHigher.setTime(yearPattern.parse(upperBoundary));
			StringBuilder textWriter = new StringBuilder();
			textWriter.append(region + "_" + id);
			textWriter.append(DELIM);
			textWriter.append(region);
			textWriter.append(DELIM);
			textWriter.append(desc);
			textWriter.append(DELIM);
			log.info(">>>>value : " + value);

			if (id.trim().substring(id.length() - 1).equalsIgnoreCase(annual)) {
				noOfCells = (endPeriod == startPeriod) ? 1
						: Integer.parseInt(endPeriod) - Integer.parseInt(startPeriod) + 1;
				log.info(">>>>IN ANNUAL : " + id);
				int counter = 4;
				keyy ="A";
				while (calendarLower.getTime().compareTo(calendarHigher.getTime()) <= 0) {
					log.info(">>>>IN while : " + id);
					if ((calendarDataLower.getTime().compareTo(calendarLower.getTime()) <= 0
							&& calendarLower.getTime().compareTo(calendarDataHigher.getTime()) <= 0)) {
						log.info(">>>>IN while IF : " + id);
						textWriter.append(token[counter]);
						textWriter.append(DELIM);
						counter++;
					} else {
						
						
						log.info(">>>>IN while ELSE : " + id);
						textWriter.append("NA");
						textWriter.append(DELIM);
						
						
						
					}

					calendarLower.add(Calendar.YEAR, 1);

				}

			} else if (id.trim().substring(id.length() - 1).equalsIgnoreCase(quarter)) {
				noOfCells = getNoOfQuartersCell(startPeriod, endPeriod);
				keyy ="Q";
				int counter = 4;
				while (calendarLower.getTime().compareTo(calendarHigher.getTime()) <= 0) {
					
					if ((calendarDataLower.getTime().compareTo(calendarLower.getTime()) <= 0
							&& calendarLower.getTime().compareTo(calendarDataHigher.getTime()) <= 0)) {
						log.info(">>>>IN while IF : " + id);
						textWriter.append(token[counter]);
						textWriter.append(DELIM);
						counter++;
					} else {
						
						
						log.info(">>>>IN while ELSE : " + id);
						textWriter.append("NA");
						textWriter.append(DELIM);
						
						
				
					}
					
					
					calendarLower.add(Calendar.MONTH, 3);
					
					
				}
			} else {
				keyy ="M";
				Long yearsInBetween = (monthPattern.parse(endPeriod).getTime()
						- monthPattern.parse(startPeriod).getTime()) / (24 * 60 * 60 * 1000);
				int counter = 4;
				while (calendarLower.getTime().compareTo(calendarHigher.getTime()) <= 0) {
					
					if ((calendarDataLower.getTime().compareTo(calendarLower.getTime()) <= 0
							&& calendarLower.getTime().compareTo(calendarDataHigher.getTime()) <= 0)) {
						log.info(">>>>IN while IF : " + id);
						textWriter.append(token[counter]);
						textWriter.append(DELIM);
						counter++;
					} else {
						
						
						log.info(">>>>IN while ELSE : " + id);
						textWriter.append("NA");
						textWriter.append(DELIM);
						
						
						
					}
					
					
					calendarLower.add(Calendar.MONTH, 1);
				}

			}
			log.info(textWriter.toString());
			if(keyy == "A"){
				multipleOutputs.write(outKey,new Text(textWriter.toString()), "IFRS9/" +"ANNUAL"+ "/part");
			}else if(keyy == "Q"){
				multipleOutputs.write(outKey,new Text(textWriter.toString()),    "IFRS9/" +"QTR"+ "/part");
			}else if(keyy == "M"){
				multipleOutputs.write(outKey,new Text(textWriter.toString()),    "IFRS9/" +"MONTH"+ "/part");
			}
			
			
			///context.write(new Text(textWriter.toString()), null);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		 multipleOutputs.close();
	}

}
