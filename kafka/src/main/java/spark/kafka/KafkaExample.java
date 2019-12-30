package spark.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.common.io.Files;

public class KafkaExample {

	public interface IKafkaConstants {
		public static String KAFKA_BROKERS = "quickstart.cloudera:9092";
		public static Integer MESSAGE_COUNT = 100;
		public static String CLIENT_ID = "client1";
		public static String TOPIC_NAME = "newsample"; // demo
		public static String GROUP_ID_CONFIG = "consumerGroup1";
		public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 200;
		public static String OFFSET_RESET_LATEST = "latest";
		public static String OFFSET_RESET_EARLIER = "earliest";
		public static Integer MAX_POLL_RECORDS = 1;
	}

	public static class ProducerCreator {
		public static Producer<String, String> createProducer() {
			Properties props = new Properties();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
					IKafkaConstants.KAFKA_BROKERS);
			props.put(ProducerConfig.CLIENT_ID_CONFIG,
					IKafkaConstants.CLIENT_ID);
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class.getName());
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class.getName());
			// props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
			// CustomPartitioner.class.getName());
			return new KafkaProducer<>(props);
		}
	}

	public static class ConsumerCreator {
		public static Consumer<String, String> createConsumer() {
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
					IKafkaConstants.KAFKA_BROKERS);
			props.put(ConsumerConfig.GROUP_ID_CONFIG,
					IKafkaConstants.GROUP_ID_CONFIG);
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class.getName());
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class.getName());
			props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
					IKafkaConstants.MAX_POLL_RECORDS);
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
					IKafkaConstants.OFFSET_RESET_EARLIER);
			Consumer<String, String> consumer = new KafkaConsumer<>(props);
			consumer.subscribe(Collections
					.singletonList(IKafkaConstants.TOPIC_NAME));
			return consumer;
		}
	}

	public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
		Instant start = Instant.now();
		runProducer();
		//runConsumer();
		
		   Instant finish = Instant.now();
	        long timeElapsed = Duration.between(start, finish).toMillis();
	        System.out.println("time required"+timeElapsed);
	}

	static void runConsumer() {
		Consumer<String, String> consumer = ConsumerCreator.createConsumer();
		int noMessageFound = 0;
		while (true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
			// 1000 is the time in milliseconds consumer will wait if no record
			// is found at broker.
			if (consumerRecords.count() == 0) {
				noMessageFound++;
				if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					// If no message found count is reached to threshold exit
					// loop.
					break;
				else
					continue;
			}
			// print each record.
			consumerRecords.forEach(record -> {
				System.out.println("consumer " + record.value());
				/*
				 * System.out.println("Record Key " + record.key());
				 * System.out.println("Record value " + record.value());
				 * System.out.println("Record partition " + record.partition());
				 * System.out.println("Record offset " + record.offset());
				 */
			});
			// commits the offset of record to broker.
			consumer.commitAsync();
		}
		consumer.close();
	}

	static void runProducer() throws ExecutionException, InterruptedException, IOException {
		
		
		{
			/*Producer<String, String> producer = ProducerCreator.createProducer();
			        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
			            ProducerRecord<String, String> record = new ProducerRecord<String, String>(IKafkaConstants.TOPIC_NAME,
			            "This is record " + index);
			            try {
			            RecordMetadata metadata = producer.send(record).get();
			            System.out.println(index+"Hi There!!");
			                        System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
			                        + " with offset " + metadata.offset());
			                 }
			            catch (ExecutionException e) {
			                     System.out.println("Error in sending record");
			                     System.out.println(e);
			                  }
			             catch (InterruptedException e) {
			                      System.out.println("Error in sending record");
			                      System.out.println(e);
			                  }
			         }
			    }
		}
}
	*/	
		final Producer<String, String> CsvProducer = ProducerCreator.createProducer();
		//Setting up a Stream to our CSV File. 
			Stream<String> FileStream = java.nio.file.Files.lines(Paths.get("/home/cloudera/Desktop/user.csv"));
			//Here we are going to read each line using Foreach loop on the FileStream object
			FileStream.forEach(line -> {
			    
			    //The topic the record will be appended to
			    //The key that will be included in the record
			    //The record contents
			    final ProducerRecord<String, String> CsvRecord = new ProducerRecord<String, String>(
			            IKafkaConstants.TOPIC_NAME, line
			    );
			    
			    // Send a record and set a callback function with metadata passed as argument.
			    CsvProducer.send(CsvRecord, (metadata, exception) -> {
			        if(metadata != null){
			            //Printing successful writes.
			        	//System.out.println(CsvRecord.value());
			            System.out.println("CsvData: -> "+ CsvRecord.key()+" | "+ CsvRecord.value());
			        }
			        else{
			            //Printing unsuccessful writes.
			            System.out.println("Error Sending Csv Record -> "+ CsvRecord.value());
			        }
			    });
			});
		}
}
}

