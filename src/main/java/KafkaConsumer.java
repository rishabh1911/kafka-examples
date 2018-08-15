import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumer {

	public static void main(String[] args) {

		Properties properties = new Properties();

		// Kafka Bootstrap Server i.e. Broker URl setting
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());

		// Add Consumer Group Id
		properties.setProperty("group.id", "test");
		// Since we are setting group id, we can run multiple consumers by re running it and check re-balancing.

		// enable auto-commit
		properties.setProperty("enable.auto.commit", "true");
		properties.setProperty("auto.commit.interval.ms", "1000");

		org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(properties);

		// Topic to read
		kafkaConsumer.subscribe(Arrays.asList("second_topic"));
		
		while(true) {
			ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
			for(ConsumerRecord<String, String> consumerRecord: consumerRecords) {
				System.out.println("Read Value: " + consumerRecord.value() + 
									"from Partition" +consumerRecord.partition());
			}
		}
	}

}
