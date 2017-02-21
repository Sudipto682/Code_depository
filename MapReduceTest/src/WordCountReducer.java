import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException
	{
		int count=0;
		for(IntWritable v:value)
		{
			count+=v.get();
		}
		context.write(key, new IntWritable(count));
	
	}

}
