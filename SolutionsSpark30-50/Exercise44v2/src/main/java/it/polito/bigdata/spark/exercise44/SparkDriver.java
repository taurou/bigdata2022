package it.polito.bigdata.spark.exercise44;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		String inputPathWatched;
		String inputPathPreferences;
		String inputPathMovies;
		String outputPath;
		double threshold;

		inputPathWatched = args[0];
		inputPathPreferences = args[1];
		inputPathMovies = args[2];
		outputPath = args[3];
		threshold = Double.parseDouble(args[4]);

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #44").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the watched movies file
		JavaRDD<String> watchedRDD = sc.textFile(inputPathWatched);

		// Select only the userid and the movieid
		// Define a JavaPairRDD with movieid as key and userid as value
		JavaPairRDD<String, String> movieUserPairRDD = watchedRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> movieUser = new Tuple2<String, String>(fields[1], fields[0]);

			return movieUser;
		});

		// Read the content of the movies file
		JavaRDD<String> moviesRDD = sc.textFile(inputPathMovies);

		// Select only the movieid and genre
		// Define a JavaPairRDD with movieid as key and genre as value
		JavaPairRDD<String, String> movieGenrePairRDD = moviesRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> movieGenre = new Tuple2<String, String>(fields[0], fields[2]);

			return movieGenre;
		});

		// Join watched movie with movies
		JavaPairRDD<String, Tuple2<String, String>> joinWatchedGenreRDD = movieUserPairRDD.join(movieGenrePairRDD);

		// Select only userid (as key) and movie genre (as value)
		JavaPairRDD<String, String> usersWatchedGenresRDD = joinWatchedGenreRDD
				.mapToPair((Tuple2<String, Tuple2<String, String>> userMovie) -> {
					// movieid - userid - genre
					Tuple2<String, String> movieGenre = new Tuple2<String, String>(userMovie._2()._1(),
							userMovie._2()._2());

					return movieGenre;
				});

		// Read the content of the preferences
		JavaRDD<String> preferencesRDD = sc.textFile(inputPathPreferences);

		// Define a JavaPairRDD with userid as key and genre as value
		JavaPairRDD<String, String> userLikedGenresRDD = preferencesRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> userGenre = new Tuple2<String, String>(fields[0], fields[1]);

			return userGenre;
		});

		// Count the number of watched movies for each user
		JavaPairRDD<String, Integer> usersNumVisualizationsRDD = usersWatchedGenresRDD.mapValues(moviegenre -> 1)
				.reduceByKey((v1, v2) -> v1 + v2);

		// Select the pairs (userid,movie-genre) that are not associated with a
		// movie-genre liked by userid
		JavaPairRDD<String, String> usersWatchedNotLikedRDD = usersWatchedGenresRDD.subtract(userLikedGenresRDD);

		// Count the number of watched movies for each user that are associated with a
		// not liked movie-genre
		JavaPairRDD<String, Integer> usersNumNotLikedVisualizationsRDD = usersWatchedNotLikedRDD
				.mapValues(moviegenre -> 1).reduceByKey((v1, v2) -> v1 + v2);

		// Join usersNumNotLikedVisualizationsRDD and usersNumVisualizationsRDD
		// and select only the users (userids) with a misleading profile with a filter
		JavaRDD<String> misleadingUsersRDD = usersNumNotLikedVisualizationsRDD.join(usersNumVisualizationsRDD)
				.filter(pair -> {
					int notLiked = pair._2()._1();
					int numWatchedMovies = pair._2()._2();

					// Check if the number of watched movies associated with a non-liked genre
					// is greater that threshold%
					if ((double) notLiked > threshold * (double) numWatchedMovies) {
						return true;
					} else
						return false;
				}).keys();

		misleadingUsersRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
