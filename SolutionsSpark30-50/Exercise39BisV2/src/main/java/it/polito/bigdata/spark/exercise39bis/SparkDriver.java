package it.polito.bigdata.spark.exercise39bis;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		//Logger.getLogger("org").setLevel(Level.OFF);
		//Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #39 bis v2"); //.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> readingsRDD = sc.textFile(inputPath).cache();

		// Apply a filter transformation to select only the lines with PM10>50
		JavaRDD<String> readingsHighValueRDD = readingsRDD.filter(PM10Reading -> {
			double PM10value;

			// Split the line in fields
			String[] fields = PM10Reading.split(",");

			// fields[2] contains the PM10 value
			PM10value = Double.parseDouble(fields[2]);

			if (PM10value > 50)
				return true;
			else
				return false;

		});

		// Create a PairRDD
		// Each pair contains a sensorId (key) and a date (value)
		// It can be implemented by using the mapToPair transformation
		JavaPairRDD<String, String> sensorsCriticalDatesRDD = readingsHighValueRDD.mapToPair(PM10Reading -> {

			String sensorID;
			String date;
			Tuple2<String, String> pair;

			// Split the line in fields
			String[] fields = PM10Reading.split(",");

			// fields[0] contains the sensorId
			sensorID = fields[0];

			// fields[1] contains the date
			date = fields[1];

			pair = new Tuple2<String, String>(sensorID, date);

			return pair;
		});

		// Create one pair for each sensor (key) with the list of
		// dates associated with that sensor (value)
		// by using the groupByKey transformation
		JavaPairRDD<String, Iterable<String>> finalSensorCriticalDates = sensorsCriticalDatesRDD.groupByKey();

		// **************************************************************************************
		// Select the sensors that have never been associated with a
		// PM10 values greater than 50
		// **************************************************************************************

		// List of distinct sensor IDs from the complete input file
		JavaRDD<String> allSensorsRDD = readingsRDD.map(PM10Reading -> PM10Reading.split(",")[0]).distinct();

		// Select the identifiers of the sensors that have never been associated with a
		// PM10 values greater than 50
		JavaRDD<String> sensorsNeverHighValueRDD = allSensorsRDD.subtract(finalSensorCriticalDates.keys());

		// Map each sensor that has never been associated with a PM10 values greater
		// than 50 to a pair (sensorId, empty list)
		JavaPairRDD<String, Iterable<String>> sensorsNeverHighValueRDDEmptyList = sensorsNeverHighValueRDD
				.mapToPair(sensorId -> new Tuple2<String, Iterable<String>>(sensorId, new ArrayList<String>()));

		// Compute the final result using union
		JavaPairRDD<String, Iterable<String>> resultRDD = finalSensorCriticalDates
				.union(sensorsNeverHighValueRDDEmptyList);

		// Store the result in the output folder
		resultRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
