import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopLineCountReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	TreeMap<Integer, Text> tmr = new TreeMap<Integer, Text>();

	public void reduce(IntWritable key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		
		for (Text t : value) {
			String s=t.toString();
	
			tmr.put(s.length(), new Text(t));
			System.out.println(t);

			if (tmr.size() > 3) {
				tmr.remove(tmr.firstKey());
			}
		}

		for (Text a : tmr.values()) {
			context.write(new IntWritable(count(a.toString())), new Text(a));
		}

	}

	private int count(String string) {
		// TODO Auto-generated method stub
		int num=0;
		for(String word:string.split(" "))
		{
			num++;
		}
		return num;
	}
	
}
