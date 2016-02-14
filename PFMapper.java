
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
public class PFMapper extends Mapper<LongWritable, Text, Text, Text> {
	Logger log = Logger.getLogger(BRMapper.class);
	private Text word = new Text();
	String[] token;
	String PF_PERSON_NUM = "";
	String PF_PLAN_NUM = "";
	String PF_SUB_PLAN = "";
	String PF_SHR_BAL_CUR = "";
	String PF_LAST_POST_DATE = "";
	String PF_BAEXT_EXT_DATE = "";
	String PF_CYC_ID = "";

	// PERSON PLAN_NUM SUB_PLAN SHR_BAL_CUR LAST_POST_DATE BAEXT_EXT_DATE CYC_ID

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		token = value.toString().split(",", -1);

		PF_PERSON_NUM = token[0];
		PF_PLAN_NUM = token[1];
		PF_SUB_PLAN = token[2];
		PF_SHR_BAL_CUR = token[3];
		PF_LAST_POST_DATE = token[4];
		PF_BAEXT_EXT_DATE = token[5];
		PF_CYC_ID = token[6];

		context.write(new Text(PF_PERSON_NUM + "~" + PF_PLAN_NUM + "~" + PF_SUB_PLAN),
				new Text(PF_SHR_BAL_CUR + "~" + PF_LAST_POST_DATE + "~" + PF_BAEXT_EXT_DATE + "~" + PF_CYC_ID+"~"+"PF_SERIES"));
	}

}
