package com.aaa.etl.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.aaa.etl.processor.Kafka2MySQLBatch;

public class EtlDataUploader2MySQL {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Kafka2MySQLBatch batch_unempl_ann = new Kafka2MySQLBatch();
		Dataset<Row> df_unempl_ann = batch_unempl_ann.getDataframe("topic_unempl_ann");
		batch_unempl_ann.saveDataframe2MySQLDB(df_unempl_ann, "table_unempl_ann");
		
		Kafka2MySQLBatch batch_house_income_ann = new Kafka2MySQLBatch();
		Dataset<Row> df_house_income_ann = batch_house_income_ann.getDataframe("topic_house_income_ann");
		batch_house_income_ann.saveDataframe2MySQLDB(df_house_income_ann, "table_house_income_ann");
		
		Kafka2MySQLBatch batch_tax_exemp_ann = new Kafka2MySQLBatch();
		Dataset<Row> df_tax_exemp_ann = batch_tax_exemp_ann.getDataframe("topic_tax_exemp_ann");
		batch_tax_exemp_ann.saveDataframe2MySQLDB(df_tax_exemp_ann, "table_tax_exemp_ann");
		
		Kafka2MySQLBatch batch_civil_force_ann = new Kafka2MySQLBatch();
		Dataset<Row> df_civil_force_ann = batch_civil_force_ann.getDataframe("topic_civil_force_ann");
		batch_civil_force_ann.saveDataframe2MySQLDB(df_civil_force_ann, "table_civil_force_ann");
		
		Kafka2MySQLBatch batch_pov_ann = new Kafka2MySQLBatch();
		Dataset<Row> df_pov_ann = batch_pov_ann.getDataframe("topic_pov_ann");
		batch_pov_ann.saveDataframe2MySQLDB(df_pov_ann, "table_pov_ann");
		
		Kafka2MySQLBatch batch_gdp_ann = new Kafka2MySQLBatch();
		Dataset<Row> df_gdp_ann = batch_gdp_ann.getDataframe("topic_gdp_ann");
		batch_gdp_ann.saveDataframe2MySQLDB(df_gdp_ann, "table_gdp_ann");
	}

}
