import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AverageMarksMapper extends Mapper<Object,Text,Text,Rec>{
	
	Rec r=new Rec();
	private Text Cid=new Text();
	
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException
	{
		String[] s=value.toString().split(" ");
		r.setCount(1);
		r.setAvg(Integer.parseInt(s[2]));
		Cid=new Text(s[1]);
		context.write(Cid, r);
		
		
	}

}
