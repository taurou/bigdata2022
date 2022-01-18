package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
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
		SparkSession ss = SparkSession.builder()
				.appName("Spark Exercise 49 - DataFrame").getOrCreate();

		// Read the content of the input file profiles.csv and store it into a
		// DataFrame
		// The input file has an header
		// Schema of the input data:
		// |-- name: string (nullable = true)
		// |-- surname: string (nullable = true)
		// |-- age: integer (nullable = true)
		Dataset<Row> profilesDF = ss.read().format("csv")
				.option("header", true).option("inferSchema", true)
				.load(inputPath);

		profilesDF.printSchema();
		profilesDF.show();

		// Define a User Defined Function called AgeCategory(Integer age)
		// that returns a string associated with the Category of the user.
		// AgeCategory = "[(age/10)*10-(age/10)*10+9]"
		// e.g.,
		// 43 -> [40-49]
		// 39 -> [30-39]
		// 21 -> [20-29]
		// 17 -> [10-19]
		// ..

		ss.udf().register("AgeCategory", (Integer age) -> {
			int min = (age / 10) * 10;
			int max = min + 9;
			return new String("[" + min + "-" + max + "]");
		}, DataTypes.StringType);

		// Define a DataFrame with the following schema:
		// |-- name: string (nullable = true)
		// |-- surname: string (nullable = true)
		// |-- rangeage: String (nullable = true)

		
		
		Dataset<Row> profilesDiscretizedAge = profilesDF
				.selectExpr("name", "surname", "AgeCategory(age) as rangeage");

		profilesDiscretizedAge.printSchema();
		profilesDiscretizedAge.show();

		// Save the result in the output folder
		// To save the results in one single file, we use the repartition method
		// to associate the Dataframe with one single partition (by setting the number of
		// partition to 1).
		profilesDiscretizedAge.write().format("csv").option("header", true)
				.save(outputPath);

		// Close the Spark session
		ss.stop();

	}
}
