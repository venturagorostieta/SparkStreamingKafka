package com.streaming.example;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;

public class RDDStreamingKafka {

	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StreamingKafkaSpark");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

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

		stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

		OffsetRange[] offsetRanges = {
				// topic, partition, inclusive starting offset, exclusive ending offset
				OffsetRange.create("test", 0, 0, 100), OffsetRange.create("test", 1, 0, 100) };

		JavaRDD<ConsumerRecord<String, String>> javaRDD = KafkaUtils.createRDD(javaSparkContext, kafkaParams,
				offsetRanges, LocationStrategies.PreferConsistent());

	
		JavaPairRDD<String, Integer> counts = javaRDD.flatMap(f -> Arrays.asList(f.value().split(" ")).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
		
		counts.saveAsTextFile("hdfs://sandbox-hdp.hortonworks.com:8020/tmp/examples/spark/streamingKafka");
		
		jssc.start();
		jssc.awaitTermination();
	}

}
