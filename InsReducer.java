
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/* author @gatripat
 * Adjustment Calculations
 */


public class InsReducer extends Reducer<Text, Text, Text, Text> {
	Logger log = Logger.getLogger(InsReducer.class);
	String busDate;
	Date dtBusDt;
	SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		busDate = conf.get("BUS_DATE");
		try {
			dtBusDt = format.parse(busDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void reduce(Text keyy, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] tokenKey = keyy.toString().split("~", -1);
		ArrayList<String> brList = new ArrayList<String>();
		ArrayList<Date> brdate = new ArrayList<Date>();
		ArrayList<String> pfList = new ArrayList<String>();
		for (Text value : values) {
			if (value.toString().contains("BR_SERIES")) {
				brList.add(value.toString());
				try {
					brdate.add(format.parse(value.toString().split("~", -1)[1]));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (value.toString().contains("PF_SERIES")) {
				pfList.add(value.toString());

			}
		}
		Collections.sort(brdate);
		int brSize = brdate.size();
		Date brTradeDate = null;
		Date pfLoadDate = null;
		int count = 0;
		for (String br : brList) {
			String[] brToken = br.split("~", -1);
			try {
				brTradeDate = format.parse(brToken[1]);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			for (String pf : pfList) {
				String[] pfToken = pf.split("~", -1);

				try {
					pfLoadDate = format.parse(pfToken[1]);
				} catch (ParseException e) {
					e.printStackTrace();
				}

				if (pfLoadDate.compareTo(dtBusDt) == 0 && count ==0) {
					count ++;
					context.write(new Text(keyy.toString() + "~" + pfToken[0] + "~" + pfToken[1] + "~" + pfToken[2]
							+ "~" + pfToken[3]), null);
				

				}
				
				if (brTradeDate.compareTo(pfLoadDate) == 0 ) {
					
					context.write(new Text(keyy.toString() + "~" + pfToken[0] + "~" + pfToken[1] + "~" + pfToken[2]
							+ "~" + pfToken[3]), null);
				} else {

					for (int j = 0; j < brSize; j++) {
						log.info("OUT LOOP : br Trade date " + brTradeDate + "  load " + pfLoadDate + " loop  br date"
								+ brdate.get(j));

						if (brdate.get(j).compareTo(brTradeDate) == 0) {
							log.info("IN LOOP : br Trade date " + brTradeDate + "  load " + pfLoadDate
									+ " loop  br date" + brdate.get(j));

							if (j == 0 && pfLoadDate.compareTo(brdate.get(j)) < 0) {
if(pfLoadDate.compareTo(dtBusDt) != 0){
	context.write(new Text(keyy.toString() + "~" + (Double.parseDouble(pfToken[0])
			+ Double.parseDouble(brToken[5])) + "~" + pfToken[1] + "~" + pfToken[2] + "~"
			+ pfToken[3]), null);
}
								
							}

							else if (pfLoadDate.compareTo(brdate.get(j)) < 0
									&& pfLoadDate.compareTo(brdate.get(j - 1)) > 0) {

								context.write(new Text(keyy.toString() + "~" + (Double.parseDouble(pfToken[0])
										+ Double.parseDouble(brToken[5])) + "~" + pfToken[1] + "~" + pfToken[2] + "~"
										+ pfToken[3]), null);
							}

						}
					}
				}

			}
		}

	}
}
