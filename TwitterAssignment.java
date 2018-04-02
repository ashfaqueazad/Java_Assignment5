package io.sparkstreamingexamples;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.twitter.*;
import org.apache.spark.streaming.api.java.*;
import twitter4j.*;
 
import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import scala.Tuple2;
 
 
public class TwitterAssignment
{
private static final Pattern SPACE = Pattern.compile(" ");
                          
public static void main(String[] args) throws Exception
  {
             
    //Twitter credentials
    final String consumerKey = "53#####5";
    final String consumerSecret = "q######F####A";
    final String accessToken = "9########I";
    final String accessTokenSecret = "7#######f";
     
    //Setting the system property using the authentication keys.
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
    System.setProperty("twitter4j.oauth.accessToken", accessToken);
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
  
         
    SparkConf sparkConf = new SparkConf()
    		.setAppName("Twitter Streaming Data handling")
    		.setMaster("local[4]")
    		.set("spark.executor.memory", "1g");
    JavaStreamingContext jsc = new JavaStreamingContext(sparkConf,  new Duration(1000));
  
    //to stop displaying messages on Spark console
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF); 
   
    //The object tweets is a DStream of tweet statuses.
    JavaDStream<Status> tweets = TwitterUtils.createStream(jsc);
  
    //the map operation on tweets maps each Status object to its text to create a new ‘transformed’ DStream named statuses. 
    JavaDStream<String> statuses = tweets.map(
    		t ->t.getText());
    
    statuses.print();
 
    //Counting and printing number of characters in each tweet.
    JavaDStream<Integer> tweetLength =statuses.map(
    		s -> s.toString().length());
    tweetLength.print();
   
    //Counting and Printing count of Words.
    JavaDStream<String> words = statuses.flatMap(
    		(String s) -> Arrays.asList(SPACE.split(s)).iterator());
    words.count();
   
    //Printing words per tweet with their occurrence times.
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
    		s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
    wordCounts.print();
   
    //total number of tweets and words
    JavaDStream<Tuple2<Integer, Integer>> characterWord = statuses
    		.map(s -> new Tuple2<>(1, (Arrays.asList(SPACE.split(s)).size())))
    		.reduceByWindow((i1,i2) -> new Tuple2<Integer,Integer>(i1._1+i2._1,i1._2+i2._2), (i1,i2) -> new Tuple2<Integer,Integer>(i1._1-i2._1,i1._2-i2._2),Durations.seconds(30), Durations.seconds(5));
    characterWord.print();
   //Average number of characters per tweet
    JavaDStream<Integer> average = characterWord.map(avg -> avg._2/avg._1);
    average.print();
   
    //apply the filter function to retain only the hashtags
    JavaDStream<String> hashTags = words.filter( t -> t.startsWith("#"));
    //counting the number of hashTags
    JavaPairDStream<String, Long> hashTagscount = hashTags.countByValue();
    hashTagscount.print();
    //
    // Getting Top 10 tweets
    JavaPairDStream<String, Integer> tuples =
    		hashTags.mapToPair(
    				(String s) -> new Tuple2<String, Integer>(s, 1));

    JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
    (Integer i1, Integer i2) -> i1 + i2,
    (Integer i1,Integer i2) -> i1 - i2,
    new Duration(60*5*1000),
    new Duration(1*1000)
    );


    JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(x-> x.swap());
    swappedCounts.print();

    JavaPairDStream<Integer, String> sortedCounts =
    swappedCounts.transformToPair(in -> in.sortByKey(false));
    sortedCounts.foreachRDD(rdd -> rdd.take(10));
    //
    
    
    //for the	last	5 minutes	of tweets,	continuously	repeat	the	computation
   // every	30 seconds.
    JavaDStream<String> tagCounts = hashTags.window(Durations.seconds(30), Durations.minutes(5));
    tagCounts.print();
    
    
    
    
    
    
    jsc.checkpoint("C:\\$$$$$$\\last");
  
    jsc.start();
    jsc.awaitTermination();
    jsc.stop();
 
  }
}