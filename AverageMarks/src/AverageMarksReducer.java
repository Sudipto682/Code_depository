import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;


public class AverageMarksReducer extends Reducer<Text,Rec,Text,Rec> {
	
	private Rec result=new Rec();
	public void reduce(Text key,Iterable<Rec>value,Context context) throws IOException, InterruptedException
	{
		float sum=0;
		int count=0;
		
		for(Rec val:value)
		{
			sum+=val.getCount()*val.getAvg();
			count+=val.getCount();
		}
		
		result.setCount(count);
		result.setAvg(sum/count);
		FloatWritable f=new FloatWritable(sum/count);
		context.write(new Text(key), result);
		
	}

}
