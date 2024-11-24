package com.aaa.etl.load;

import com.aaa.etl.processor.Hdfs2Kafka;

public class EtlDataUploader2Kafka {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Hdfs2Kafka hdfs2kafka = new Hdfs2Kafka();
		
		hdfs2kafka.readHdFile("unemployee_annual.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_unempl_ann", str));
		hdfs2kafka.getHdFilesInfo("unemployee_annual.csv");
		
		hdfs2kafka.readHdFile("household_income.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_house_income_ann", str));
		hdfs2kafka.getHdFilesInfo("household_income.csv");
		
		hdfs2kafka.readHdFile("tax_exemption.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_tax_exemp_ann", str));
		hdfs2kafka.getHdFilesInfo("tax_exemption.csv");
		
		hdfs2kafka.readHdFile("civilian_force.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_civil_force_ann", str));
		hdfs2kafka.getHdFilesInfo("civilian_force.csv");
		
		hdfs2kafka.readHdFile("poverty.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_pov_ann", str));
		hdfs2kafka.getHdFilesInfo("poverty.csv"); 
		
		hdfs2kafka.readHdFile("real_gdp.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_gdp_ann", str));
		hdfs2kafka.getHdFilesInfo("real_gdp.csv");
		
		hdfs2kafka.readHdFile("unemployee_monthly.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_unempl_mon", str));
		hdfs2kafka.getHdFilesInfo("unemployee_monthly.csv");
		
		hdfs2kafka.readHdFile("earnings_Construction.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_earn_Construction_mon", str));
		hdfs2kafka.getHdFilesInfo("earnings_Construction.csv");
		
		hdfs2kafka.readHdFile("earnings_Education_and_Health_Services.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_earn_Education_and_Health_Services_mon", str));
		hdfs2kafka.getHdFilesInfo("earnings_Education_and_Health_Services.csv");
		
		hdfs2kafka.readHdFile("earnings_Financial_Activities.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_earn_Financial_Activities_mon", str));
		hdfs2kafka.getHdFilesInfo("earnings_Financial_Activities.csv");
		
		hdfs2kafka.readHdFile("earnings_Goods_Producing.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_earn_Goods_Producing_mon", str));
		hdfs2kafka.getHdFilesInfo("earnings_Goods_Producing.csv");
		
		hdfs2kafka.readHdFile("earnings_Leisure_and_Hospitality.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_earn_Leisure_and_Hospitality_mon", str));
		hdfs2kafka.getHdFilesInfo("earnings_Leisure_and_Hospitality.csv");
		
		hdfs2kafka.readHdFile("earnings_Manufacturing.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_earn_Manufacturing_mon", str));
		hdfs2kafka.getHdFilesInfo("earnings_Manufacturing.csv");
		
		hdfs2kafka.readHdFile("earnings_Private_Service_Providing.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_earn_Private_Service_Providing_mon", str));
		hdfs2kafka.getHdFilesInfo("earnings_Private_Service_Providing.csv");
		
		hdfs2kafka.readHdFile("earnings_Professional_and_Business_Services.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_earn_Professional_and_Business_Services_mon", str));
		hdfs2kafka.getHdFilesInfo("earnings_Professional_and_Business_Services.csv");
		
		hdfs2kafka.readHdFile("earnings_Trade_Transportation_and_Utilities.csv").forEach(str ->
				hdfs2kafka.sendLines2Kafka("topic_earn_Trade_Transportation_and_Utilities_mon", str));
		hdfs2kafka.getHdFilesInfo("earnings_Trade_Transportation_and_Utilities.csv");
		
		hdfs2kafka.closeStream();
	}
}
