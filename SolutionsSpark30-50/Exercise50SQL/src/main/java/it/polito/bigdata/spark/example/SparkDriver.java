package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SparkDriver {

	public static void main(String[] args) {
		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a Spark Session object and set the name of the application
		// SparkSession ss = SparkSession.builder().appName("Spark Exercise 49 - DataFrame").getOrCreate();
		SparkSession ss = SparkSession.builder().master("local").appName("Spark Exercise 49 - DataFrame").getOrCreate();

		// Read the content of the input file profiles.csv and store it into a
		// DataFrame
		// The input file has an header
		// Schema of the input data:
		// |-- name: string (nullable = true)
		// |-- surname: string (nullable = true)
		// |-- age: integer (nullable = true)
		Dataset<Row> profilesDF = ss.read().format("csv").option("header", true).option("inferSchema", true)
				.load(inputPath);

		// Assign the “table name” profiles to the profilesDF DataFrame
		profilesDF.createOrReplaceTempView("profiles");

		// Define a User Defined Function called Concatenate(String name, String
		// surname)
		// that returns a string associated with the concatenation of name and
		// surname.
		// e.g.,
		// Paolo, Garza -> "Paolo Garza"
		// ..

		ss.udf().register("Concatenate", (String name, String surname) -> new String(name + " " + surname),
				DataTypes.StringType);

		// Define a DataFrame with the following schema:
		// |-- name_surname: string (nullable = true)

		Dataset<Row> namesDF = ss.sql("SELECT Concatenate(name, surname) as name_surname FROM profiles");

		// Save the result in the output folder
		// To save the results in one single file, we use the repartition method
		// to associate the Dataframe with one single partition (by setting the
		// number of
		// partition to 1).
		namesDF.write().format("csv").option("header", true).save(outputPath);

		// Close the Spark session
		ss.stop();

	}
}
