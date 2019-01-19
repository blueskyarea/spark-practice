package com.blueskyarea;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloDataFrame {
	Logger logger = LoggerFactory.getLogger(HelloDataFrame.class);
    public static void main( String[] args ) {
        new HelloDataFrame().execute();
    }
    
    private void execute() {
    	SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[3]");
		sparkConf.setAppName("HelloDataFrame");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);
		DataFrame df = sqlContext.read().json("src/main/resources/people.json");
		logger.info("show");
		df.show();
		logger.info("printSchema");
		df.printSchema();
		logger.info("select name");
		df.select("name").show();
		logger.info("col");
		df.select(col("name"), col("age").plus(1)).show();
		logger.info("filter");
		df.filter(df.col("age").gt(20)).show();
		logger.info("groupBy");
		df.groupBy("age").count().show();
    }
}
