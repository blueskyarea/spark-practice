package com.blueskyarea;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingSaveAsTextFile {

	private static final String FILTER_WORD = "ERROR";
	
	public static void main(String[] args) {
		// create StreamingContext
		SparkConf conf = new SparkConf().setAppName("StreamingSaveAsTextFile");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		// create DStream from text file
		String logDir = "/tmp/logs3";
		JavaDStream<String> logData = jssc.textFileStream(logDir);
		
		// Filter by specified word
		JavaDStream<String> filtered = logData.filter(new Function<String, Boolean>() {

			public Boolean call(String line) throws Exception {
				return line.contains(FILTER_WORD);
			}
			
		});
		
		// output
		filtered.print(10);
		
		// save
		filtered.foreachRDD(new Function<JavaRDD<String>, Void>() {
			public Void call(JavaRDD<String> rdd) throws Exception {
				if(!rdd.isEmpty()) {
					rdd.saveAsTextFile("/tmp/output");
				}
				return null;
			}
		});
		
		// start streaming
		jssc.start();
		
		// wait for end of job
		jssc.awaitTermination();
	}

}
