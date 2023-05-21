package com.blueskyarea;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingApp {
    public static void main( String[] args ) {
    	// create StreamingContext
    	SparkConf conf = new SparkConf().setAppName("StreamingApp");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        
        // create DStream from specified port
        JavaDStream<String> lines = jssc.socketTextStream("localhost", 7777);
        
        // filter for DStream
        JavaDStream<String> errorLines = lines.filter(new Function<String, Boolean>() {
			public Boolean call(String line) throws Exception {
				return line.contains("error");
			}
        });
        
        // output
        errorLines.print();
        
        // start streaming
        jssc.start();
        
        // wait for end of job
        jssc.awaitTermination();
    }
}
