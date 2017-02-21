import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopLineCountMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	private TreeMap<Integer, Text> tm = new TreeMap<Integer, Text>();
	int id=0;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String s = value.toString();
		//System.out.println("*************"+ value);
		int count = 0;
		for (String str : s.split(" ")) {
			count += 1;
		}

		//tm.put(count, new Text(value.toString()));
		tm.put(count, new Text(value));

		if (tm.size() > 3) {
			tm.remove(tm.firstKey());
		}
		
		
		
		
	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		//Set<Map.Entry<Integer,Text>> set=tm.entrySet();
		
		
		
/*
		for (Map.Entry<Integer,Text>me:set) {
			System.out.println(me.getKey()+"**************"+tm.get(me.getKey())); 
		
			context.write(new IntWritable(me.getKey()),me.getValue());*/
		
		for(Text t:tm.values())
		{
			System.out.println(t);
			context.write(new IntWritable(1), new Text(t));
		}
	}

}
