package com.home;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.*;

import scala.Tuple2;
import twitter4j.Status;

public class SparkStream {
    public static final Pattern SPACE =Pattern.compile(" ");
    public static void main(String[] args)  {


        String host="localhost";
        int port = 9999;

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaTwitterHashTagJoinSentiments");


        String consumerKey = "#############";
        String consumerSecret = "#############";
        String accessToken = "##############-##################";
        String accessTokenSecret = "####################";
        String[] filters = {"hadoop"};

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf,Durations.seconds(1));
        JavaReceiverInputDStream<Status> lines = TwitterUtils.createStream(jsc,filters);
        jsc.checkpoint("/tmp/checkpoint_nw");

        //JavaReceiverInputDStream<String> lines= jsc.socketTextStream(host, port,StorageLevels.MEMORY_AND_DISK_SER);

        JavaDStream<String> words= lines.flatMap(x->Arrays.asList(x.getText().split(" ")).iterator());

        JavaPairDStream<String, Integer> wordcounts= words.mapToPair(x->new Tuple2<String, Integer>(x,1));

        JavaPairDStream<String, Integer> counts=wordcounts.reduceByKey((x1,x2)->x1+x2);

        counts.checkpoint(Durations.seconds(2));

        counts.print();

        jsc.start();

        jsc.awaitTermination();


    }

}
