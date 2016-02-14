
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


/* author @gatripat
 * Adjustment Calculations
 */
public class BRMapper extends Mapper<LongWritable, Text, Text, Text> {
	Logger log = Logger.getLogger(BRMapper.class);
	private Text word = new Text();
	String[] token;
	String BR_RUN_DATE = "";
	String BR_TRADE_DATE = "";
	String BR_PERSON_NUM = "";
	String BR_PLAN_NUM = "";
	String BR_SUB_PLAN = "";
	String BR_ACTIVITY = "";
	String BR_FUND_IV = "";
	String BR_FUND_SRC = "";
	String BR_SHARES = "";
	String BR_SHRCOST = "";
	String BR_SHR_PRICE = "";
	String BR_POST_NUM = "";

	// PERSON PLAN_NUM SUB_PLAN SHR_BAL_CUR LAST_POST_DATE BAEXT_EXT_DATE CYC_ID

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		token = value.toString().split(",", -1);

		BR_RUN_DATE = token[0];
		BR_TRADE_DATE = token[1];
		BR_PERSON_NUM = token[2];
		BR_PLAN_NUM = token[3];
		BR_SUB_PLAN = token[4];
		BR_ACTIVITY = token[5];
		BR_FUND_IV = token[6];
		BR_FUND_SRC = token[7];
		BR_SHARES = token[8];
		BR_SHRCOST = token[9];
		BR_SHR_PRICE = token[10];
		BR_POST_NUM = token[11];

		context.write(new Text(BR_PERSON_NUM + "~" + BR_PLAN_NUM + "~" + BR_SUB_PLAN),
				new Text(BR_RUN_DATE + "~" + BR_TRADE_DATE + "~" + BR_ACTIVITY + "~" + BR_FUND_IV + "~" + BR_FUND_SRC
						+ "~" + BR_SHARES + "~" + BR_SHRCOST + "~" + BR_SHR_PRICE + "~" + BR_POST_NUM + "~"+"BR_SERIES"));
	}

}
