import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DriverCode {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if(args.length<2)
		{
			System.out.println("Give input and output directories properly");
		}
		
		Job job=new Job();
		job.setJarByClass(DriverCode.class);
		job.setJobName("Average using Combiner");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AverageMarksMapper.class);
		job.setReducerClass(AverageMarksReducer.class);
		job.setCombinerClass(AverageMarksReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Rec.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Rec.class);
		
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
