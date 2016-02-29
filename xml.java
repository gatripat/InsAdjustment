package com.barclays.ifrs9;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Document;

public class xml {

	String[] token;
	String id = "";
	String startPeriod = "";
	String endPeriod = "";
	String desc = "";
	String frequency = "";
	static String lowerBoundary = "1978";
	static String upperBoundary = "2020";
	static Integer boundaryPointer = 0;
	static String annual = "A";
	static String month = "M";
	static String quarter = "Q";
	static String DELIM = "|";
	String region = "";
	HashMap<String, String> data = new HashMap<String, String>();
	static SimpleDateFormat yearPattern = new SimpleDateFormat("yyyy");
	static SimpleDateFormat monthPattern = new SimpleDateFormat("yyyy-MMM");
	static SimpleDateFormat weekPattern = new SimpleDateFormat("yyyy-MMM-dd");
	// SimpleDateFormat quarterPattern = new SimpleDateFormat("yyyy-MMM");
	Integer noOfCells = 0;
	Calendar calendarDataLower;
	Calendar calendarDataHigher;

	public void refineData(String style, String inPath, String out) throws Exception {
		File stylesheet1 = new File(style);

		File xmlSource = new File(inPath);

		DocumentBuilderFactory factory1 = DocumentBuilderFactory.newInstance();

		DocumentBuilder builder1 = factory1.newDocumentBuilder();

		Document document1 = builder1.parse(xmlSource);

		StreamSource stylesource1 = new StreamSource(stylesheet1);

		Transformer transformer1 = TransformerFactory.newInstance().newTransformer(stylesource1);

		Source source1 = new DOMSource(document1);

		Result outputTarget1 = new StreamResult(new File(out));

		transformer1.transform(source1, outputTarget1);

	}

	public static void main(String args[]) throws Exception {
		xml x = new xml();
		x.refineData(args[0], args[1], args[2]);

		//x.refineData("C://style.xsl", "C://in.xml", "C://out.csv");
		
		
		
		
		Calendar calendarLower = Calendar.getInstance();
		calendarLower.setTime(yearPattern.parse(lowerBoundary));

		Calendar calendarLower1 = Calendar.getInstance();
		calendarLower1.setTime(yearPattern.parse(lowerBoundary));

		Calendar calendarHigher = Calendar.getInstance();
		calendarHigher.setTime(yearPattern.parse(upperBoundary));

		

		String str;
		PrintWriter annual = new PrintWriter( args[3]+"/ANNUAL");
		PrintWriter month = new PrintWriter(args[3]+"/MONTH");
		PrintWriter quarter = new PrintWriter(args[3]+"/QTR");
		BufferedReader in = new BufferedReader(new FileReader(args[2]));
		try {
			StringBuilder sb = new StringBuilder();
			String key = "";
			while ((str = in.readLine()) != null) {
				
				String startPeriod = str.split("\\|", -1)[13].trim().split(":",-1)[0];
				String endPeriod =str.split("\\|", -1)[str.split("\\|", -1).length -1].trim().split(":",-1)[0];
				Calendar calendarDataLower = Calendar.getInstance();
				calendarDataLower.setTime(yearPattern.parse(startPeriod));
				Calendar calendarDataHigher = Calendar.getInstance();
				calendarDataHigher.setTime(yearPattern.parse(endPeriod));
System.out.println("LL "+calendarDataLower.getTime());
System.out.println("HH "+calendarDataHigher.getTime());
				sb.append(str.split("\\|", -1)[0]);
				sb.append("|");
				sb.append(str.split("\\|", -1)[1]);
				sb.append("|");
				sb.append(str.split("\\|", -1)[2]);
				sb.append("|");
				int counter = 13;
				System.out.println("IN WHILE  => "+str.split("\\|", -1)[2]);
				if (str.split("\\|", -1)[2].equals("A")) {
					System.out.println("IN WHILE "+calendarLower.getTime() + "  "+ calendarHigher.getTime());
					while (calendarLower.getTime().compareTo(calendarHigher.getTime()) <= 0) {
						System.out.println("IN WHILE"+str.split("\\|", -1)[2]);
						if ((calendarDataLower.getTime().compareTo(calendarLower.getTime()) <= 0
								&& calendarLower.getTime().compareTo(calendarDataHigher.getTime()) <= 0)) {
							
							System.out.println("---->");
							
							sb.append(str.split("\\|", -1)[counter].split(":", -1)[1]);
							sb.append("|");
							counter++;
						} else {
							sb.append("NA");
							sb.append("|");
						}
						calendarLower.add(Calendar.YEAR, 1);
					}
					annual.append(sb.toString());

				}
				if (str.split("\\|", -1)[2].equals("M")) {

					while (calendarLower.getTime().compareTo(calendarHigher.getTime()) <= 0) {
						if ((calendarDataLower.getTime().compareTo(calendarLower.getTime()) <= 0
								&& calendarLower.getTime().compareTo(calendarDataHigher.getTime()) <= 0)) {
							sb.append(str.split("\\|", -1)[counter].split(":", -1)[1]);
							sb.append("|");
							counter++;
						} else {
							sb.append("NA");
							sb.append("|");
						}
						calendarLower.add(Calendar.MONTH, 1);
					}
					month.append(sb.toString());

				}
				if (str.split("|", -1)[2].equals("Q")) {

					while (calendarLower.getTime().compareTo(calendarHigher.getTime()) <= 0) {
						if ((calendarDataLower.getTime().compareTo(calendarLower.getTime()) <= 0
								&& calendarLower.getTime().compareTo(calendarDataHigher.getTime()) <= 0)) {
							sb.append(str.split("\\|", -1)[counter].split(":", -1)[1]);
							sb.append("|");
							counter++;
						} else {
							sb.append("NA");
							sb.append("|");
						}
						calendarLower.add(Calendar.MONTH, 3);
					}
					quarter.append(sb.toString());

				}

			}
		

		
		} finally {
			in.close();
			annual.close();
			month.close();
			quarter.close();
		}

	}

	private Integer getNoOfQuartersCell(String startYearQuartrComplex, String endYearQuartrComplex) {
		int count = 0;

		Integer startYear = Integer.parseInt(startYearQuartrComplex.split("-", -1)[0]);
		Integer startQuarterNumber = Integer.parseInt(startYearQuartrComplex.split("-", -1)[1]);

		Integer endYear = Integer.parseInt(endYearQuartrComplex.split("-", -1)[0]);
		Integer endQuarterNumber = Integer.parseInt(endYearQuartrComplex.split("-", -1)[1]);

		// Integer startQuarterNumber =
		// Integer.parseInt(startQuatr.trim().substring(startQuatr.length() -
		// 1));
		// Integer endQuarterNumber =
		// Integer.parseInt(endQuatr.trim().substring(endQuatr.length() - 1));
		int yrcount = (startYear == endYear) ? (endYear - startYear) * 4 : (endYear - startYear - 1) * 4;
		count = yrcount + (((1 + endQuarterNumber) + (4 - startQuarterNumber)));
		return count;

	}

}
