import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LineWordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String s=value.toString();
		int count=0;
		for(String word:s.split(" "))
		{
			count+=1;
		}
		
		context.write(value, new IntWritable(count));
	}

}
