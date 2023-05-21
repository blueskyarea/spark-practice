package com.blueskyarea;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileUtil;
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

public class ReadLocalTextTest {
	private File tempDir;
	private String filePath;
	private SparkSession ss;
	
	@Before
	public void setUp() throws IOException {
		// Create directory
		tempDir = Files.createTempDir();
		
		// Define a text file
		filePath = tempDir.getAbsolutePath().concat("/file1.txt");
		
		// Spark configuration
		SparkConf sparkConf = new SparkConf().setAppName("ReadLocalTextTest").setMaster("local[*]")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.set("spark.io.compression.codec", "lz4");
		ss = SparkSession.builder().config(sparkConf).getOrCreate();
	}
	
	@After
	public void cleanup() {
		if (ss != null) {
			ss.stop();
			ss = null;
		}
		FileUtil.fullyDelete(tempDir);
	}
	
	@Test
	public void readTextFileTest() {
		// Create a text file on local
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
				.schema(schema).csv(filePath.toString())
				.cache();
		
		assertThat(ds.count(), is(2L));
		assertThat(ds.filter(ds.col("id").equalTo(1)).first().getAs("message"), is("This is first line."));
		assertThat(ds.filter(ds.col("id").equalTo(2)).first().getAs("message"), is("This is second line."));
		ds.show();
	}
	
	private void createTextFile() {
		List<String> lines = Stream.of(
				"1\tThis is first line.",
				"2\tThis is second line.").collect(Collectors.toList());
		
		try (BufferedWriter writer = java.nio.file.Files.newBufferedWriter(Paths.get(filePath))) {
            for (String line : lines) {
                writer.append(line);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
}
