package com.barclays.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/* author @gatripat
 * Adjustment Calculations
 */

public class CSVparserDriver {

	
	
	public void write(String ip, String data, String hdfsFile) throws IOException, URISyntaxException {
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI("hdfs://" + ip), configuration);
		byte[] byt = data.getBytes();
		FSDataOutputStream fsOutStream = hdfs.create(new Path(hdfsFile));
		fsOutStream.write(byt);
		fsOutStream.close();
		System.out.println("Written data to HDFS file.");
	}
	
	
	private String refineData(String inPath) {
		StringBuilder sb1 = new StringBuilder();	
		try {

			BufferedReader in = new BufferedReader(new FileReader(inPath));
			String str;
			StringBuilder sb = new StringBuilder();
			while ((str = in.readLine()) != null) {
				sb.append(str.replaceAll("\\)", "\\),"));
				sb.append("~");

			}
			in.close();
			//System.out.println("Main " + sb.toString());
			String x = sb.toString().replace(",~", ",");
			String[] y = x.split(",", -1);
			
			String pattern = "[0-9]+~[A-Z]+";
			Pattern r = Pattern.compile(pattern);
			String find = "";
			for (String xx : y) {
				Matcher m = r.matcher(xx);
				if (m.find()) {
					find = xx.replace("~", "\n");
					sb1.append(find);
					sb1.append("~");
				}else{

				sb1.append(xx);
				sb1.append(",");
				}

			}


		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		return sb1.toString().replace("~,", ",").replace("~", ",");
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(CSVparserDriver.class);
		job.setMapperClass(CSVparserMapper.class);

		CSVparserDriver driver = new CSVparserDriver();
		String data = driver.refineData(args[0]);
		driver.write(args[5], data, args[1]);
		
		// job.setNumReduceTasks(0);
		job.setJobName("IFRS CSV Parser PoC");

		job.getConfiguration().set("LOWER_DATE", args[3].toString());
		job.getConfiguration().set("HIGHER_DATE", args[4].toString());

		// map-reduce job.

		job.setJarByClass(CSVparserDriver.class);
		job.setMapperClass(CSVparserMapper.class);

		job.setNumReduceTasks(0);

		// map-reduce job.
		FileInputFormat.addInputPath(job, new Path(args[1]));

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Submit the job and wait for it to finish.
		job.waitForCompletion(true);
	}

}
