package consumerapi;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;



public class consumertest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String topic="java_api_test";
		String groupname="Supplier";
		Properties props=new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", groupname);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, String> consumer=new KafkaConsumer<String,String>(props);
		consumer.subscribe(Arrays.asList(topic));
		while(true)
		{
			ConsumerRecords<String, String> records=consumer.poll(100);
			for(ConsumerRecord<String, String>rec:records)
			{
				System.out.println(rec.key()+"*********"+rec.value());
			}
		}
	}

}
