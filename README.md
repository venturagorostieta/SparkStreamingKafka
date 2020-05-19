# SparkStreamingKafka
Example of Streaming using Kafka 1.1.1 and Spark streaming 2.3

For execute example, it´s required create topic "kafka-test"


spark-submit \
  --class "com.streaming.example.DirectStreaming" \
  --master local[*] \
/tmp/spark/streaming/JavaStreamingDirect-jar-with-dependencies.jar
