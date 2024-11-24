package com.aaa.etl.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.aaa.etl.processor.Kafka2MongoStream;

public class EtlDataUploader2MongoDB {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Kafka2MongoStream stream2Mongo = new Kafka2MongoStream();
		
		Dataset<Row> df_unempl_month = stream2Mongo.getDataframe("topic_unempl_mon");
		stream2Mongo.saveDataframe2MongoDB(df_unempl_month, "coll_unempl_month");
		
		Dataset<Row> df_earn_Construction_month = stream2Mongo.getDataframe("topic_earn_Construction_mon");
		stream2Mongo.saveDataframe2MongoDB(df_earn_Construction_month, "coll_earn_Construction_month");
		
		Dataset<Row> df_education_and_Health_Services_month = stream2Mongo.getDataframe("topic_earn_Education_and_Health_Services_mon");
		stream2Mongo.saveDataframe2MongoDB(df_education_and_Health_Services_month, "coll_earn_Education_and_Health_Services_month");
		
		Dataset<Row> df_financial_Activities_month = stream2Mongo.getDataframe("topic_earn_Financial_Activities_mon");
		stream2Mongo.saveDataframe2MongoDB(df_financial_Activities_month, "coll_earn_Financial_Activities_month");
		
		Dataset<Row> df_goods_Producing_month = stream2Mongo.getDataframe("topic_earn_Goods_Producing_mon");
		stream2Mongo.saveDataframe2MongoDB(df_goods_Producing_month, "coll_earn_Goods_Producing_month");
		
		Dataset<Row> df_leisure_and_Hospitality_month = stream2Mongo.getDataframe("topic_earn_Leisure_and_Hospitality_mon");
		stream2Mongo.saveDataframe2MongoDB(df_leisure_and_Hospitality_month, "coll_earn_Leisure_and_Hospitality_month");
		
		Dataset<Row> df_manufacturing_month = stream2Mongo.getDataframe("topic_earn_Manufacturing_mon");
		stream2Mongo.saveDataframe2MongoDB(df_manufacturing_month, "coll_earn_Manufacturing_month");
		
		Dataset<Row> df_private_Service_Providing_month = stream2Mongo.getDataframe("topic_earn_Private_Service_Providing_mon");
		stream2Mongo.saveDataframe2MongoDB(df_private_Service_Providing_month, "coll_earn_Private_Service_Providing_month");
		
		Dataset<Row> df_professional_and_Business_Services_month = stream2Mongo.getDataframe("topic_earn_Professional_and_Business_Services_mon");
		stream2Mongo.saveDataframe2MongoDB(df_professional_and_Business_Services_month, "coll_earn_Professional_and_Business_Services_month");
		
		Dataset<Row> df_trade_Transportation_and_Utilities_month = stream2Mongo.getDataframe("topic_earn_Trade_Transportation_and_Utilities_mon");
		stream2Mongo.saveDataframe2MongoDB(df_trade_Transportation_and_Utilities_month, "coll_earn_Trade_Transportation_and_Utilities_month");
		
		stream2Mongo.getSparkSession().streams().awaitAnyTermination();
	}

}