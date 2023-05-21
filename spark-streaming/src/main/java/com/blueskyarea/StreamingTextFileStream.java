package com.blueskyarea;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StreamingTextFileStream {

	public static void main(String[] args) {
		// create StreamingContext
		SparkConf conf = new SparkConf().setAppName("StreamingTextFileStream");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		// create DStream from text file
		String logDir = "/tmp/logs";
		JavaDStream<String> logData = jssc.textFileStream(logDir);
		
		// Split into words
		JavaDStream<String> words = logData.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		
		// Transform into word and count
		JavaPairDStream<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2(word, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer original, Integer additional) throws Exception {
				return original + additional;
			}
		});
		
		// output
		counts.print(50);
		
		// start streaming
		jssc.start();
		
		// wait for end of job
		jssc.awaitTermination();
	}

}
