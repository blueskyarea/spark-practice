package com.blueskyarea;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
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

public class ReadHDFSTextTest {
	private File tempDir;
	private String c1PathStr;
	private String filePath;

	//private Configuration conf;
	private FileSystem fs;
	private MiniDFSCluster cluster;
	private SparkSession ss;

	/*@BeforeClass
	public static void setupTests() throws Exception {
		// Force logging level for a job class
		LogManager.getLogger(ReadHDFSTextTest.class).setLevel(Level.DEBUG);
	}*/

	@Before
	public void setup() throws Exception {
		// Create directory
		tempDir = Files.createTempDir();
		c1PathStr = tempDir.getAbsolutePath().concat("/cluster1");
		filePath = c1PathStr.concat("/").concat("dfile1.txt");

		// HDFS configuration
		Configuration conf = new HdfsConfiguration();
		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1PathStr);
		cluster = new MiniDFSCluster.Builder(conf).build();
		fs = FileSystem.get(conf);

		// Spark configuration
		SparkConf sparkConf = new SparkConf().setAppName("ReadHDFSTextTest").setMaster("local[*]")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.set("spark.io.compression.codec", "lz4");
		ss = SparkSession.builder().config(sparkConf).getOrCreate();
	}

	@After
	public void cleanup() {
		if (cluster != null) {
			cluster.shutdown();
			cluster = null;
		}
		if (ss != null) {
			ss.stop();
			ss = null;
		}
		FileUtil.fullyDelete(tempDir);
	}

	@Test
	public void readTextFileTest() throws IOException {
		// Create a text file on HDFS
		createTextFile();
		
		// Define the text file schema
		StructType schema = new StructType(
				new StructField[] { new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
						new StructField("message", DataTypes.StringType, false, Metadata.empty()) });
		
		// Read the text file
		Dataset<Row> ds = ss.read()
				.option("header", false)
				.option("delimiter", "\t")
				.schema(schema).csv("hdfs://" + fs.getCanonicalServiceName() + "/" + filePath.toString())
				.cache();
		assertThat(ds.count(), is(2L));
		assertThat(ds.filter(ds.col("id").equalTo(1)).first().getAs("message"), is("This is first line."));
		assertThat(ds.filter(ds.col("id").equalTo(2)).first().getAs("message"), is("This is second line."));
		ds.show();
	}
	
	private void createTextFile() throws IllegalArgumentException, IOException {
		FSDataOutputStream out = fs.create(new Path(filePath));
		
		// Required \n for writing with change line
		List<String> lines = Stream.of("1\tThis is first line.\n", "2\tThis is second line.")
				.collect(Collectors.toList());
		for (String line : lines) {
			out.writeBytes(line);
		}
		out.close();
	}
}
