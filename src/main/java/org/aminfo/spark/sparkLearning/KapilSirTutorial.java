package org.aminfo.spark.sparkLearning;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KapilSirTutorial {

	public static void main(String[] args) {
		
		SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("KapilSirTutorials")
				.getOrCreate(); // Unified gateway for spark

		Dataset<Row> csv = sparkSession.read().option("header", true).csv("/home/ist/Desktop/ip.csv");
		csv.show(false);

		csv.createOrReplaceTempView("IPs");
		Dataset<Row> result = sparkSession.sql("SELECT Uncompressed FROM IPs WHERE compressed='-'");
		result.show(false);

		Dataset<String> textData = sparkSession.read().textFile("/home/ist/Desktop/ip.csv").cache();
		textData.show(false);

		String showString = textData
				.filter((org.apache.spark.api.java.function.FilterFunction<String>) s -> s.contains(":"))
				.showString(1, 0, true);
		long count = textData.filter((org.apache.spark.api.java.function.FilterFunction<String>) s -> s.contains(":"))
				.count();
		System.out.println(showString);
		System.out.println(count);
	}
}
