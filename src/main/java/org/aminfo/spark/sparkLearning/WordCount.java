package org.aminfo.spark.sparkLearning;

import java.time.LocalDateTime;
import java.util.Arrays;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount {
	public static void main(String[] args) {

		Logger log = LoggerFactory.getLogger(WordCount.class);

		SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("WordCount").getOrCreate();

		if (args.length < NumberUtils.INTEGER_ONE) {
			log.warn("Using default configurations to read input.");
			args = new String[2];
			args[0] = "/home/ist/Desktop/ip.csv";
			args[1] = "/home/ist/Desktop/tt/" + LocalDateTime.now().getMinute();
		}
		log.info("Reading file {}", args[0]);
		Dataset<String> stringDS = sparkSession.read().textFile(args[0]).as(Encoders.STRING());

		Dataset<String> flatMap = stringDS.flatMap((FlatMapFunction<String, String>) s -> {
			return Arrays.asList(((String) s).split("")).iterator();
		}, Encoders.STRING()).coalesce(1);

		log.debug("Please find below schema of the flat map");
		flatMap.printSchema();

		Dataset<Row> countAgainstword = flatMap.groupBy("value").count().toDF("word", "count");
		countAgainstword.sort(functions.desc("count"));
		
		log.debug("Below is the data set");
		countAgainstword.show(false);

		countAgainstword.toJavaRDD().saveAsTextFile(args[1]);
		
		log.info("Finish !");

	}

}
