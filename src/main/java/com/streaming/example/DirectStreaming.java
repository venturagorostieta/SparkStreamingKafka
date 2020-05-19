package com.streaming.example;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class DirectStreaming {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws InterruptedException {
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StreamingKafkaSpark");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		jssc.sparkContext().setLogLevel("ERROR");

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");// 1 or more brokers
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "java-stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("kafka-test");// 1 o more topics

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		// stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

		JavaDStream<String> lines = stream.map(ConsumerRecord::value);
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
				.reduceByKey((i1, i2) -> i1 + i2);
		
		wordCounts.print();

		jssc.start();
		jssc.awaitTermination();

	}
}
