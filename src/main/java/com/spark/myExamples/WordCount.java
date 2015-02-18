package com.spark.myExamples;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author chihoub
 *
 */
public class WordCount {
	
	static String inputFile = "/Users/chihoub/workspace/tweetCollection/tweets_1424084445000/part-00000";

	public static void main(String[] args) {
		
    	if (args.length != 1){
    		System.out.println("Usage: " + Thread.currentThread().getStackTrace()[1].getClassName() + "<inputfile>");
    		System.out.println("Default value will be used");
    		//System.exit(1);
    	}else{
    		inputFile = args[0];  		
    	}
		
		SparkConf sparkConf = new SparkConf()
        .setAppName("Tweets analysis");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        JavaRDD<String> file  = sc.textFile(inputFile);
        JavaPairRDD<String, Integer> wordCount= file.flatMap(s -> Arrays.asList(s.split(" ")))
        		.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
        		.reduceByKey(  (Integer a, Integer b) -> a + b);
        
        /********* Scala equivalent code is really elegant 
         * val counts = file.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
         */
        
        wordCount.collect().forEach(System.out::println);


	}

}
