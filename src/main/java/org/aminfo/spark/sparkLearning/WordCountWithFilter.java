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

public class WordCountWithFilter {

	public static void main(String[] args) {
		Logger log = LoggerFactory.getLogger(WordCountWithFilter.class);

		SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("WordCount").getOrCreate();

		if (args.length < NumberUtils.INTEGER_ONE) {
			log.warn("Using default configurations to read input.");
			args = new String[2];
			args[0] = "/home/ist/Desktop/ip.csv";
			args[1] = "/home/ist/Desktop/tt/" + LocalDateTime.now().getMinute();
		}
		log.info("Reading file {}", args[0]);

		Dataset<String> fileRawData = sparkSession.read().textFile(args[0]).as(Encoders.STRING());
		/*
		 * Dataset<String> flatMap = fileRawData.flatMap(new FlatMapFunction() {
		 * 
		 * @Override public Iterator call(Object t) throws Exception { return (Iterator)
		 * Arrays.asList(((String) t).split("")).iterator(); } },
		 * Encoders.STRING()).coalesce(1);
		 */

		Dataset<String> flatMap = fileRawData.flatMap((FlatMapFunction<String, String>) s -> {
			return Arrays.asList(((String) s).split("")).iterator();
		}, Encoders.STRING()).coalesce(1);

		flatMap.printSchema();

		Object[] vowels = { "a", "e", "i", "o", "u" };
		Dataset<String> filteredDataSet = flatMap.filter(flatMap.col("value").isin(vowels));
		Dataset<Row> finalDataSet = filteredDataSet.groupBy("value").count().toDF("word", "count");
		finalDataSet.printSchema();
		finalDataSet = finalDataSet.sort(functions.desc("count"));
		finalDataSet.show(false);
		log.info("Writing data at location: {}", args[1]);
		finalDataSet.toJavaRDD().saveAsTextFile(args[1]);

	}

}
