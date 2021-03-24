package org.aminfo.spark.sparkLearning;

import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaStreaming {

	public static void main(String[] args) throws StreamingQueryException, TimeoutException {
		Logger log = LoggerFactory.getLogger(KafkaStreaming.class);

		String masters = "local[*]";
		String appName = "PUBLISH KAFKA EVENT";
		String kafkaPath;
		String kafkaTopicName;
		
		if (args.length < NumberUtils.INTEGER_ONE) {
			log.warn("Using default configurations of kafka.");
			args = new String[2];
			kafkaTopicName = "TOPIC1";
			kafkaPath = "localhost:9092";
		} else {
			kafkaPath = args[0];
			kafkaTopicName = args[1];
		}
		log.info("\n Kafka path: {} \n Kafka Topic: {}", kafkaPath, kafkaTopicName);

		SparkSession spark = SparkSession.builder().master(masters).appName(appName).getOrCreate();
		log.info("Spark session created.");

		Dataset<Row> rawKafkaDataSet = spark.readStream().format("kafka").option("kafka.bootstrap.servers", kafkaPath)
				.option("subscribe", kafkaTopicName).option("startingOffsets", "earliest").load();
		log.info("Kafka Connection establised.");

		Dataset<Row> eventDataSet = rawKafkaDataSet.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
				"CAST(topic AS STRING)", "CAST(partition AS STRING)", "CAST(offset AS STRING)");

		StreamingQuery eventMonitor = eventDataSet.writeStream().outputMode("append").format("console").start();
		log.info("Kafka listner started");

		eventMonitor.awaitTermination();
	}
}