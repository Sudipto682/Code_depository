package kafkaproducer;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String topic="kafkaIntegration";
		Random random =new Random();
		/*String key="key";
		String value="value1"*/;
		
		Properties props=new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		
		KafkaProducer<String, String> producer=new KafkaProducer<String,String>(props);
		
		for(int i=0;i<50;i++)
		{
			int r=random.nextInt(10);
			ProducerRecord<String, String>record=new ProducerRecord<String, String>(topic, Integer.toString(r),Integer.toString(r));
			producer.send(record);
		}
		
		producer.close();
		System.out.println("Completed");

	}

}
