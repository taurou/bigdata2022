package it.polito.bigdata.spark.exercise48;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.count;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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
		SparkSession ss = SparkSession.builder().master("local").appName("Spark Exercise #48 - DataFrame").getOrCreate();

		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: name,age,gender
		// The input file has an header. Hence, the name of the columns of
		// the defined DataFrame will be name, age, gender
		Dataset<Row> dfProfiles = ss.read().format("csv").option("header", true).option("inferSchema", true)
				.load(inputPath);


		// Define a Dataset of Profile objects from the dfProfiles DataFrame
		Dataset<Profile> dsProfiles = dfProfiles.as(Encoders.bean(Profile.class));

		
		// printSchema and show only for debug purposes
		// They can be removed
		dsProfiles.printSchema();
		dsProfiles.show();

		// Compute avg(age) and count(*) for each name
		Dataset<NameAvgAgeCount> dsNameAvgAgeCount = dfProfiles
				.groupBy("name").agg(avg("age"),count("*"))
				.withColumnRenamed("avg(age)", "avgage")
				.withColumnRenamed("count(1)", "count").as(Encoders.bean(NameAvgAgeCount.class));

		// Select the names with at least two occurrences 
		Dataset<NameAvgAgeCount> dsSeletedRecords = dsNameAvgAgeCount.
				filter(record -> record.getCount()>=2);
		
		// Select only columns name and avg(age)
//		Dataset<NameAvgAge> dsSeletedNames = dsSeletedRecords.select("name", "avgage")
//				.as(Encoders.bean(NameAvgAge.class));
		Dataset<NameAvgAge> dsSeletedNames = dsSeletedRecords.map( 
				(NameAvgAgeCount record) -> {
			NameAvgAge newRecord = new NameAvgAge();
			newRecord.setName(record.getName());
			newRecord.setAvgage(record.getAvgage());
			return newRecord;	
		}, Encoders.bean(NameAvgAge.class));
		
		// printSchema and show only for debug purposes
		// They can be removed
		dsSeletedNames.printSchema();
		dsSeletedNames.show();
		
		dsSeletedNames.write().format("csv").option("header", false).save(outputPath);

		// Close the Spark context
		ss.stop();
	}
}
