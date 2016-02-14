
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/* author @gatripat
 * Adjustment Calculations
 */

public class InsDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(InsDriver.class);
		// job.setMapperClass(PFMapper.class);
		job.setReducerClass(InsReducer.class);
		// job.setNumReduceTasks(0);
		job.setJobName("Participant Adjustment PoC");

		String busDate = args[3].toString();
		job.getConfiguration().set("BUS_DATE", busDate);

		// map-reduce job.
		Path inputPath1 = new Path(args[0]);
		Path inputPath2 = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, PFMapper.class);
		MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, BRMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		// TODO: Update the output path for the output directory of the
		// map-reduce job.
		// configuration should contain reference to your namenode

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Submit the job and wait for it to finish.
		job.waitForCompletion(true);
	}

}
