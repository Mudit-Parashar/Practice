package streaming.spark;

import java.util.*;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

public class App {

	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf().setAppName("spark-kafka-test")
				.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(
				600000));
		//Set<String> topics = new HashSet<String>();
			//topics = Collections.singleton("newsample");
			// Set<String> topics = (Set) Collections.singleton("mytopic");
			Map<String, Object> kafkaParams = new HashMap<>();
			kafkaParams.put("bootstrap.servers", "quickstart.cloudera:9092");
			kafkaParams.put("key.deserializer", StringDeserializer.class);
			kafkaParams.put("value.deserializer", StringDeserializer.class);
			kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
			kafkaParams.put("auto.offset.reset", "latest");
			kafkaParams.put("enable.auto.commit", false);
		//	kafkaParams.put("offsets.manage","false");
			//JavaInputDStream<ConsumerRecord<String, String>> stream
			Collection<String> topics = Arrays.asList("newsample");
			JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils
					.createDirectStream( ssc , LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));	
			
							
							
							
							/*ssc, String.class, String.class,
							StringDecoder.class, StringDecoder.class,
							kafkaParams, topics);*/
			  
			
			kafkaStream.foreachRDD(rdd -> {
				System.out.println("--- New RDD with " + rdd.partitions().size()
						+ " partitions and " + rdd.count() + " records");
				rdd.foreach(record -> 
				System.out.println(record));

			});
			ssc.start();
			ssc.awaitTermination();

		
	}
	/*
	 * SparkConf conf = new
	 * SparkConf().setAppName("sparkkafka").setMaster("local[*]");
	 * JavaSparkContext sc = new JavaSparkContext(conf); JavaStreamingContext
	 * jsc = new JavaStreamingContext(sc, new
	 * org.apache.spark.streaming.Duration(1000));
	 * 
	 * Map<String, String> kafkaParams = new HashMap<>();
	 * 
	 * kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
	 * kafkaParams.put("key.deserializer", StringDeserializer.class);
	 * kafkaParams.put("value.deserializer", StringDeserializer.class);
	 * kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
	 * kafkaParams.put("auto.offset.reset", "latest");
	 * kafkaParams.put("enable.auto.commit", false);
	 * 
	 * Collection<String> topics = Arrays.asList("topicA", "topicB");
	 * 
	 * final JavaInputDStream<ConsumerRecord<String, String>> stream =
	 * KafkaUtils.createDirectStream( streamingContext,
	 * LocationStrategies.PreferConsistent(), ConsumerStrategies.<String,
	 * String>Subscribe(topics, kafkaParams) );
	 * 
	 * stream.mapToPair( new PairFunction<ConsumerRecord<String, String>,
	 * String, String>() {
	 * 
	 * @Override public Tuple2<String, String> call(ConsumerRecord<String,
	 * String> record) { return new Tuple2<>(record.key(), record.value()); } })
	 */

	/*
	 * Collection<String> topics = Arrays.asList("test4");
	 * 
	 * //HashMap kafkaprops1 = new HashMap();
	 * 
	 * HashMap kafkaParams = new HashMap();
	 * 
	 * //kafkaParams.put("bootstrap.servers" , "quickstart.cloudera:9092");\
	 * 
	 * kafkaprops.put("bootstrap.servers", "localhost:9092");
	 * kafkaprops.put("group.id","xx"); kafkaprops.put("key.deserializer",
	 * "org.apache.kafka.common.serialization.IntegerDeserializer");
	 * kafkaprops.put("value.deserializer",
	 * "org.apache.kafka.common.serialization.StringDeserializer");
	 * 
	 * 
	 * JavaInputDStream<ConsumerRecord<String, String>> ds =
	 * KafkaUtils.createDirectStream( jsc,
	 * LocationStrategies.PreferConsistent(), ConsumerStrategies.<String,
	 * String>Subscribe(topics, kafkaParams));
	 * 
	 * // Subscribe(topics, kafkaParams)); JavaDStream<String> jds = ds.map(x ->
	 * x.value());
	 * 
	 * jds.foreachRDD(rdd -> { rdd.foreachPartition(partitionOfRecords -> {
	 * 
	 * Producer<Integer, String> producer = MyKafkaProducer.getProducer(); while
	 * (partitionOfRecords.hasNext()) { producer.send(new
	 * ProducerRecord<>("test", 1, partitionOfRecords.next())); }
	 * 
	 * }); });
	 * 
	 * jsc.start(); jsc.awaitTermination();
	 */
}
