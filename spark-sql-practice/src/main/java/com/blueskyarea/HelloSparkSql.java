package com.blueskyarea;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloSparkSql{
	Logger logger = LoggerFactory.getLogger(HelloSparkSql.class);
	
    public static void main( String[] args )    {
        new HelloSparkSql().execute();
    }
    
    private void execute() {
    	SparkSession spark = SparkSession
    			  .builder()
    			  .appName("Java Spark SQL basic example")
    			  .master("local[3]")
    			  .getOrCreate();
    	
    	// read json
    	Dataset<Row> df = spark.read().json("src/main/resources/people.json");
    	
    	logger.info("--- show ---");
    	df.show();
    	
    	logger.info("--- printSchema ---");
    	df.printSchema();
    	
    	logger.info("--- select name ---");
    	df.select("name").show();
    	
    	logger.info("--- select all plus ---");
    	df.select(col("name"), col("age").plus(1)).show();
    	
    	logger.info("--- filter ---");
    	df.filter(col("age").gt(20)).show();
    	
    	logger.info("--- groupBy ---");
    	df.groupBy("age").count().show();
    }
}
