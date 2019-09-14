import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Task3Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Task3Driver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 6) {
			System.err.println("Usage: TASK1 <in> <out>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}

		Job job = new Job(conf,
				"total count of records grouped by Variable 2and Variable 5for all rows where the value of Variable9 is greater than 5.50");
		job.setJarByClass(Task3Driver.class);
		job.setMapperClass(Task3Mapper.class);
		job.setReducerClass(Task3Reducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		String inputPath = "hdfs://localhost:54310//sqoop/Project//olympics_History//part-m-00000";
		String outPutPath = "hdfs://localhost:54310//sqoop/Project//olympics_History//Task3";
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[3]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[4]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[5]));
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

}
