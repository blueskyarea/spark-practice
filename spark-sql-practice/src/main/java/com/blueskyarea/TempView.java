package com.blueskyarea;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TempView {
	Logger logger = LoggerFactory.getLogger(TempView.class);

	public static void main(String[] args) throws AnalysisException {
		new TempView().execute();
	}

	private void execute() throws AnalysisException {
		SparkSession spark = SparkSession
				.builder()
				.appName("TempView")
				.master("local[3]")
				.getOrCreate();
		
		// read json
		Dataset<Row> df = spark.read().json("src/main/resources/people.json");
		
		logger.info("--- Register the DataFrame as a SQL temporary view ---");
		df.createOrReplaceTempView("people");
		
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();
		
		logger.info("--- Create new session for temporary view ---");
		//spark.newSession().sql("SELECT * FROM people").show();
		// -> org.apache.spark.sql.AnalysisException: Table or view not found: people
		
		logger.info("--- Register the DataFrame as a global temporary view ---");
		df.createGlobalTempView("people");
		
		spark.sql("SELECT * FROM global_temp.people").show();
		
		logger.info("--- Create new session for global temporary view ---");
		spark.newSession().sql("SELECT * FROM global_temp.people").show();
	}
}
