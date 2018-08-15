import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducer {

	public static void main(String[] args) {
		
		Properties properties = new Properties();
		
		// Kafka Bootstrap Server i.e. Broker URl setting
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		
		//producer acknowledgement settings
		properties.setProperty("acks", "1");
		properties.setProperty("retries", "3");
		
		Producer<String, String> kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);
		
		//topic name associated with producer record
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("second_topic", "myKey", "MyFourthMessage");
		// message with a given key goes to same partition given number of partitions remain constant.
		
		kafkaProducer.send(producerRecord);
		
		// send all messages it has before closing
		kafkaProducer.flush();
		kafkaProducer.close();
		
		System.out.println("Producer Closed.");
	}

}
