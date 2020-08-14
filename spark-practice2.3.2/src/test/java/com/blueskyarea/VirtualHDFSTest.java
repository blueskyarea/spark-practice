package com.blueskyarea;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.spark_project.guava.io.Files;

public class VirtualHDFSTest {
	private File tempDir;
	private String filePath;
	private MiniDFSCluster hdfsCluster;
	private SparkSession ss;
	
	@Before
	public void setUp() throws IOException {
		// Create directory
		tempDir = Files.createTempDir();
		// Define a text file
		filePath = tempDir.getAbsolutePath().concat("/file1.txt");
		
		// Create virtual HDFS cluster with temporary directory
		Configuration hadoopConf = new Configuration();
		hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hadoopConf);
		hdfsCluster = builder.build();
		
		// Spark configuration
		SparkConf sparkConf = new SparkConf().setAppName("VirtualHDFSTest").setMaster("local[*]");
		ss = SparkSession.builder().config(sparkConf).getOrCreate();
	}
	
	@After
	public void cleanup() {
		ss.close();
		hdfsCluster.shutdown();
		FileUtil.fullyDelete(tempDir);
	}
	
	@Test
	public void readTextFile() throws IOException {
		// Create a text file
		createTextFile();
		
		// Define the text file schema
		StructType schema = new StructType(new StructField[] {
			new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("message", DataTypes.StringType, false, Metadata.empty())
		});
		
		// Read the text file
		Dataset<Row> ds = ss.read()
				.option("header", false)
				.option("delimiter", "\t")
				.schema(schema).csv(filePath)
				.cache();
		
		assertThat(ds.count(), is(2L));
		assertThat(ds.filter(ds.col("id").equalTo(1)).first().getAs("message"), is("This is first line."));
		assertThat(ds.filter(ds.col("id").equalTo(2)).first().getAs("message"), is("This is second line."));
	}
	
	private void createTextFile() throws IOException {
		try (FileWriter writer = new FileWriter(new File(filePath))) {
			BufferedWriter bw = new BufferedWriter(writer);
			bw.write("1\tThis is first line.");
			bw.newLine();
			bw.write("2\tThis is second line.");
			bw.flush();
		}
	}
}
