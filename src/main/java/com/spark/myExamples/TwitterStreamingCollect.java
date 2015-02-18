package com.spark.myExamples;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import com.google.gson.Gson;

/**
 * @author chihoub
 *
 */
public class TwitterStreamingCollect implements Serializable {
    
    static long numTweetsCollected = 0L;
    static Gson gson = new Gson();
    static String outputDir = "/Users/chihoub/workspace/tweetCollection";
    
    public static void main(String[] args) {
    	
    	if (args.length != 1){
    		System.out.println("Usage: " + Thread.currentThread().getStackTrace()[1].getClassName() + "<outputDir>");
    		System.out.println("Default value will be used");
    		//System.exit(1);
    	}else{
    		outputDir = args[0];  		
    	}
        
        // Twitter4J
        // Put API keys in the twitter4J.properties file
        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);
        
        
        // Spark
        SparkConf sparkConf = new SparkConf()
        .setAppName("Tweets collection");
        //.setMaster("local[2]");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(15000));
        
        // Stream and save filtered tweets
        String[] filters = { "#CiteUneSerieQuiTaMarquer" };
        TwitterUtils.createStream(sc, twitterAuth, filters).map(s -> gson.toJson(s))
        .foreachRDD((rdd, time) -> {
            long count = rdd.count();
            if (count > 0) {
                //JavaRDD<String> outputRDD = rdd.repartition(4);
                rdd.saveAsTextFile(outputDir + "/tweets_" + Long.toString(time.milliseconds()));
                numTweetsCollected += count;
                System.out.println("Number of collected tweets: " + numTweetsCollected);
            }
            return null;
        });

        
        
        sc.start();
        sc.awaitTermination();
    }
}
