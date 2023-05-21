package com.blueskyarea;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingTextFileStreamMulti {

	public static void main(String[] args) {
		// create StreamingContext
		SparkConf conf = new SparkConf().setAppName("StreamingTextFileStreamMulti");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		// create DStream from text file
		String logDir = "/tmp/logs";
		String logDir2 = "/tmp/logs2";
		JavaDStream<String> logData = jssc.textFileStream(logDir);
		JavaDStream<String> logData2 = jssc.textFileStream(logDir2);
		
		// output
		logData.print();
		logData2.print();
		
		// start streaming
		jssc.start();
		
		// wait for end of job
		jssc.awaitTermination();
	}

}
