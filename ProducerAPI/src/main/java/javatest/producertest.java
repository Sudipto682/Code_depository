package javatest;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.producer.Producer;

public class producertest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String topic="java_api_test";
		String key="key";
		String value="value1";
		
		Properties props=new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String, String> producer=new KafkaProducer<String,String>(props);
		ProducerRecord<String, String>record=new ProducerRecord<String, String>(topic, key,value);
		producer.send(record);
		producer.close();
		System.out.println("Completed");
		
	}

}
