package com.aaa.etl.processor;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_csv;
import static org.apache.spark.sql.functions.not;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.aaa.etl.pojo.StoredColumnPojo;
import com.aaa.etl.util.PropertyFileReader;
import com.mongodb.spark.MongoSpark;

import scala.Predef;
import scala.collection.JavaConverters;

public class Kafka2MongoStream {

	private Properties systemProp;
		
	private SparkSession spark;
	
	public Kafka2MongoStream() throws Exception{
		systemProp = PropertyFileReader.readPropertyFile("SystemConfig.properties");
		
		String appName = (String)systemProp.get("spark.stream.name");
		spark = SparkSession.builder().master("local[*]").appName(appName).getOrCreate();
	}
		
	private static <A,B> scala.collection.immutable.Map<A, B> toScalaMap(Map<A, B> m) {
		return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(Predef.$conforms());
	}
		
	public SparkSession getSparkSession() {
		if(spark != null) {
			return spark;
		}
			
		return null;
	}
		
	public Dataset<Row> getDataframe(String kafkaTopic) {
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("kafka.bootstrap.servers", systemProp.getProperty("kafka.brokerlist"));
		kafkaParams.put("subscribe", kafkaTopic);
		kafkaParams.put("startingOffsets", systemProp.getProperty("kafka.resetType"));
			
		Dataset<Row> df = spark.readStream().format("kafka")
				.options(kafkaParams)
				.load()
				.selectExpr("CAST(value AS STRING) as column").filter(not(col("column").startsWith("date")));
			
		return df;
	}
			
	public void saveDataframe2MongoDB(Dataset<Row> df, String collection) throws Exception{
		Map<String, String> options = new HashMap<String, String>();
				
		Dataset<Row> dfs = df.select(from_csv(col("column"), StoredColumnPojo.getStructType(), toScalaMap(options))
				.as("entityStoredPojo"))
				.selectExpr("entityStoredPojo.date", "entityStoredPojo.value", "entityStoredPojo.id", 
							"entityStoredPojo.title", "entityStoredPojo.state", "entityStoredPojo.frequency_short", 
							"entityStoredPojo.units_short", "entityStoredPojo.seasonal_adjustment_short").toDF();
				
		dfs.printSchema();
				
		String mongoUri = (String)systemProp.get("mongodb.output.uri") 
				+ (String)systemProp.get("mongodb.output.database")
				+ "." + collection;
				
		dfs.writeStream().foreachBatch(
				(each_df, batchId) -> {
					MongoSpark.save(each_df.write().option("uri", mongoUri).mode("overwrite"));
		}).start(); 
			
		/////////////////////////////////////////////////////////////
		//   저장소가 MySQL인 경우는 아래와 같이 코딩
		/*String url = systemProp.getProperty("mysql.output.uri");
		String table = collection;   
		String user = systemProp.getProperty("mysql.username");
		String password = systemProp.getProperty("mysql.password");
			
		Properties jdbcProps = new Properties();
		jdbcProps.put("user", user);
		jdbcProps.put("password", password);
			
		dfs.writeStream()
			.foreachBatch((each_df, batchId) -> {
					each_df.write().mode("append").jdbc(url, table, jdbcProps);
			}).start(); */
	}
}