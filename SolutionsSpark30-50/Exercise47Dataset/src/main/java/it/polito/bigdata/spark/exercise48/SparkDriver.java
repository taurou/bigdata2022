package it.polito.bigdata.spark.exercise48;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;

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
		// SparkSession ss = SparkSession.builder().appName("Spark Exercise #48 - DataFrame").getOrCreate();
		SparkSession ss = SparkSession.builder()
				.master("local").appName("Spark Exercise #48 - Dataset")
				.getOrCreate();

		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: name,age,gender
		// The input file has an header. Hence, the name of the columns of
		// the defined DataFrame will be name, age, gender
		Dataset<Row> dfProfiles = ss.read()
				.format("csv").option("header", true).option("inferSchema", true)
				.load(inputPath);

		// Define a Dataset of Profile objects from the dfReading DataFrame
		Dataset<Profile> dsProfiles = dfProfiles.as(Encoders.bean(Profile.class));

		// printSchema and show only for debug purposes
		// They can be removed
		dsProfiles.printSchema();
		dsProfiles.show();

		// Select only male users
		// Non type-safe version of the filter operation
		// Dataset<Profile> dsProfilesMale =	dsProfiles.filter("gender='male'");

		// Type-safe version of the filter operation
		Dataset<Profile> dsProfilesMale = dsProfiles
				.filter(p -> p.getGender().compareTo("male")==0);

		// printSchema and show only for debug purposes
		// They can be removed
		dsProfilesMale.printSchema();
		dsProfilesMale.show();

		// Select only the columns name and age
		// Update also age (age = age+1)
		// Non type-safe version of the filter operation
		// Dataset<Row> dsProfilesMaleIncreasedAge = dsProfilesMale.selectExpr("name","age+1 as age");

		// Type-safe version based on the map operation
		Dataset<NameAgeProfile> dsProfilesMaleIncreasedAge = dsProfilesMale
				.map(p -> new NameAgeProfile(p.getName(), p.getAge() + 1), 
						Encoders.bean(NameAgeProfile.class));

		// printSchema and show only for debug purposes
		// They can be removed
		dsProfilesMaleIncreasedAge.printSchema();
		dsProfilesMaleIncreasedAge.show();

		// Sort data by age (descending), name (ascending)
		// Only the non type-safe version is available for this operation
		Dataset<NameAgeProfile> dsProfilesMaleIncreasedAgeSorted = dsProfilesMaleIncreasedAge
				.sort(new Column("age").desc(),
				new Column("name"));

		// printSchema and show only for debug purposes
		// They can be removed
		dsProfilesMaleIncreasedAgeSorted.printSchema();
		dsProfilesMaleIncreasedAgeSorted.show();
		
		dsProfilesMaleIncreasedAgeSorted.write().format("csv").option("header", false).save(outputPath);

		// Close the Spark context
		ss.stop();
	}
}
