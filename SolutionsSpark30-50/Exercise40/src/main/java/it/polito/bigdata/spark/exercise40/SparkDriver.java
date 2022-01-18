package it.polito.bigdata.spark.exercise40;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #40");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> readingsRDD = sc.textFile(inputPath);

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
		// Each pair contains a sensorId (key) and 1 (value)
		// It can be implemented by using the mapToPair transformation
		JavaPairRDD<String, Integer> sensorsCriticalRDD = readingsHighValueRDD.mapToPair(PM10Reading -> {

			String sensorID;
			Tuple2<String, Integer> pair;

			// Split the line in fields
			String[] fields = PM10Reading.split(",");

			// fields[0] contains the sensorId
			sensorID = fields[0];

			pair = new Tuple2<String, Integer>(sensorID, new Integer(1));

			return pair;
		});

		// Count the number of occurrences of each sensor
		// by using the reduceByKey transformation
		JavaPairRDD<String, Integer> sensorNumCriticalValuesRDD = 
				sensorsCriticalRDD.reduceByKey((i1, i2) -> i1 + i2);

		// Invert the role of key and value.
		// (sensorId, num. critical days) -> (num.critical days, sensorId)
		// It is useful to use the sortByKey transformation on the
		// new PairRDD
		JavaPairRDD<Integer, String> numCriticalValuesSensorRDD = sensorNumCriticalValuesRDD
				.mapToPair((Tuple2<String, Integer> inPair) -> 
					new Tuple2<Integer, String>(inPair._2(), inPair._1()));

		// Use sortByKey to sort the pairs by key in descending oder
		JavaPairRDD<Integer, String> sortedNumCriticalValuesSensorRDD = 
				numCriticalValuesSensorRDD.sortByKey(false);

		sortedNumCriticalValuesSensorRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
