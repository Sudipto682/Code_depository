import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DriverCode {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
		
		if(args.length<2)
		{
			System.out.println("Enter input and output directories properly");
			System.exit(-1);
		}
		
		Job job=new Job();
		job.setJarByClass(DriverCode.class);
		job.setJobName("Line_Word_Count");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(LineWordCountMapper.class);
		//job.setReducerClass(LineWordCountReducer.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		

	}

}
