import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class LineWordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException
	{
		int count=0;
		for(IntWritable i:value)
		{
			if(count<2)
			{
				context.write(key, i);
				count++;
			}
		}
	}

}
