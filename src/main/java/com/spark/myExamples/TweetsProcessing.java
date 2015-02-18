package com.spark.myExamples;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

/**
 * @author chihoub
 *
 */
public class TweetsProcessing implements Serializable {
    
    static JsonParser jsonParser = new JsonParser();
    static Gson gson = new GsonBuilder().setPrettyPrinting().create();
    static HashingTF tf = new HashingTF(1000);
    static int nClusters = 3, nIterations = 20;
    static String inputDir = "/Users/chihoub/workspace/tweetCollection/tweets_1424084445000/"; //just not to use ENV variable everytime
    
    public static void main(String[] args) {
        
        

    	if (args.length != 3){
    		System.out.println("Usage: " + Thread.currentThread().getStackTrace()[1].getClassName() +
    	     "<inputDir> <number of clusters> <number of iterations>");
    		System.out.println("Default values will be used");
    		//System.exit(1);
    	}else{
    		inputDir = args[0];
    		nClusters = Integer.parseInt(args[1]);
    		nIterations = Integer.parseInt(args[2]);    		
    	}
        
        // Spark configuration
        SparkConf sparkConf = new SparkConf()
        .setAppName("Tweets analysis");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaSQLContext sqlContext = new JavaSQLContext(sc);
        
        //Pretty print the tweets
        JavaRDD<String> tweets = sc.textFile("/Users/chihoub/workspace/tweetCollection/tweets_1424084445000/", 6);
        tweets.map(s -> gson.toJson(jsonParser.parse(s)))
        .collect()
        .forEach(System.out::println);
        
        /***************************           Spark SQL         ******************************************************/
        JavaSchemaRDD tTable = sqlContext.jsonRDD(tweets).cache();
        tTable.registerTempTable("tweetTable");
        
        System.out.println("------Tweet table Schema---");
        tTable.printSchema();
        
        System.out.println("----Sample Tweet Text-----");
        sqlContext.sql("SELECT text FROM tweetTable LIMIT 10")
        .collect()
        .forEach(System.out::println);
        
        System.out.println("------Sample Lang, Name, text---");
        sqlContext.sql("SELECT user.lang, user.name, text FROM tweetTable LIMIT 20")
        .collect()
        .forEach(System.out::println);
        
        System.out.println("------Total count by languages Lang, count(*)---");
        sqlContext.sql("SELECT user.lang, COUNT(*) as cnt FROM tweetTable GROUP BY user.lang ORDER BY cnt DESC LIMIT 20")
        .collect()
        .forEach(System.out::println);
        

        /***************************           Spark ML         ******************************************************/
        //JavaRDD<String[]> texts = sqlContext.sql("SELECT text from tweetTable").map(s -> s.toString().split(" "));
        JavaRDD<List<String>> texts = sqlContext.sql("SELECT text from tweetTable").map(s -> Arrays.asList(s.toString()));
        JavaRDD<Vector> vectors = tf.transform(texts).cache();
        vectors.collect().forEach(System.out::println);
       
        KMeansModel model = KMeans.train(vectors.rdd(), nClusters, nIterations);
        System.out.println("         clustering model         : " + model.toString());
        
        
        //Small illustration
        
        texts.collect().forEach(s -> {
    		System.out.println(" This tweet    " + s.toString()  + "    is predicted to be in cluster  : " + model.predict(tf.transform(s)));
        });
        
        
        
        
        
    
    }
    
}
