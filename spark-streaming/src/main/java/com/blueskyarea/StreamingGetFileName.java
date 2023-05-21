package com.blueskyarea;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingGetFileName {

	public static void main(String[] args) {
		// create StreamingContext
		SparkConf conf = new SparkConf().setAppName("StreamingGetFileName");

		try (JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(10))) {
			// create DStream from text file
			String logDir = "/tmp/logs";
			JavaDStream<String> logData = jssc.textFileStream(logDir);

			// try to get file name
			logData.foreachRDD(rdd -> {
				Pattern p = Pattern.compile("file:.*?\\s+");
				Matcher m = p.matcher(rdd.toDebugString());
				if (m.find()) {
					System.out.println(m.group());
				}
				return null;
			});

			// start streaming
			jssc.start();

			// wait for end of job
			jssc.awaitTermination();
		}
	}
}
