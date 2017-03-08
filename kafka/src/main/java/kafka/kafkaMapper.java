package kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class kafkaMapper extends Mapper<Object,Text,Text,IntWritable> {

	public void map(Object key,Text value,Context context) throws IOException, InterruptedException
	{
		String topic="kafkaIntegration";
		String groupname="User";
		Properties prop=new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("group.id", groupname);
		
		KafkaConsumer<String,String> consumer=new KafkaConsumer<String,String>(prop);
		consumer.subscribe(Arrays.asList(topic));
		while(true)
		{
			ConsumerRecords<String, String> records=consumer.poll(50);
			for(ConsumerRecord<String, String>rec:records)
			{
				context.write(new Text(rec.value().toString()),new IntWritable(1));
			}
		}
	}
}
