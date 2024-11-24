package com.aaa.etl.load;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties kafkaConsProperty = new Properties();
		// 환경 변수 설정
		kafkaConsProperty.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		kafkaConsProperty.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaConsProperty.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaConsProperty.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
		kafkaConsProperty.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		Consumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaConsProperty); // consumer 생성
		//////////////////////////////////////////////////////////////////
		// topic_unempl_ann
		// topic_house_income_ann
		// topic_tax_exemp_ann
		// topic_civil_force_ann
		// topic_pov_ann
		// topic_gdp_ann
		// topic_unempl_mon
		// topic_earn_Construction_mon
		// topic_earn_Education_and_Health_Services_mon
		// topic_earn_Financial_Activities_mon
		// topic_earn_Goods_Producing_mon
		// topic_earn_Leisure_and_Hospitality_mon
		// topic_earn_Manufacturing_mon
		// topic_earn_Private_Service_Providing_mon
		// topic_earn_Professional_and_Business_Services_mon
		// topic_earn_Trade_Transportation_and_Utilities_mon
		// 위 토픽 값 중에 하나를 택하여 토픽을 설정합니다.
		
		consumer.subscribe(Collections.singletonList("topic_unempl_ann"));
		
		String message = null;
		
		try {
			while(true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
		
				for (ConsumerRecord<String, String> record : records) {
					message = record.value();
					System.out.println(message);
				}
			}
		} catch(Exception e) {
			// exception
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
