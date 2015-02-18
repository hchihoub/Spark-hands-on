package com.spark.myExamples;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

public class TwitterStreaming{
    
    public static void main(String[] args) {
        
        // Twitter4J
        // Put your API keys in the twitter4J.properties file
        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);
        
        // Spark
        SparkConf sparkConf = new SparkConf()
        .setAppName("Tweets Android");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(15000));
        
        // Some processing on tweets
        String[] filters = { "#GRAMMYs" };
        TwitterUtils.createStream(sc, twitterAuth, filters)
        .flatMap(s -> Arrays.asList(s.getHashtagEntities()))
        .map(h -> h.getText().toLowerCase())
        .filter(h -> !h.equals("grammys"))
        .countByValue()
        .print();
        
        sc.start();
        sc.awaitTermination();
    }
}
