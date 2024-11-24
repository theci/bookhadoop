package com.aaa.etl.processor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.aaa.etl.util.PropertyFileReader;

public class Hdfs2Kafka {

	private Properties systemProp = null;
	private FileSystem hadoopFs = null;
	
	private Properties kafkaProdProperty; // 이 라인을 입력하여 멤버변수를 추가합니다.
	private Producer<String, String> kafkaProducer; // 이 라인을 입력하여 멤버변수를 추가합니다.
	
	public Hdfs2Kafka() throws Exception {
		systemProp = PropertyFileReader.readPropertyFile("SystemConfig.properties");
		String HADOOP_CONF_DIR = systemProp.getProperty("hadoop.conf.dir");
	
		Configuration conf = new Configuration();
		conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/core-site.xml"));
		conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/hdfs-site.xml"));
	
		String namenode = systemProp.getProperty("hdfs.namenode.url");
		hadoopFs = FileSystem.get(new URI(namenode), conf);
	
		// 생성자의 뒷부분에 아래의 kafkaProdProroperty 객체를 생성합니다.
		kafkaProdProperty = new Properties();
		kafkaProdProperty.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, systemProp.get("kafka.brokerlist"));
		kafkaProdProperty.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		kafkaProdProperty.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		kafkaProdProperty.put(ProducerConfig.ACKS_CONFIG, "all");
		kafkaProdProperty.put(ProducerConfig.RETRIES_CONFIG, Integer.valueOf(1));
		kafkaProdProperty.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.valueOf(20000));
		kafkaProdProperty.put(ProducerConfig.LINGER_MS_CONFIG, Integer.valueOf(1));
		kafkaProdProperty.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Integer.valueOf(133554432));
		
		// KafkaProducer 객체를 생성합니다.
		kafkaProducer = new KafkaProducer<String, String>(kafkaProdProperty);
	}
	
	public List<String> readHdFile(String filename) throws Exception {
		Path path = new Path(filename);
		List<String> lines = new ArrayList<>();
	
		if(!hadoopFs.exists(path)) {
			System.out.println("파일이 존재하지 않습니다.");
			return null;
		}
	
		FSDataInputStream input = hadoopFs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(input));
	
		while(br.ready()) {
			String[] line = br.readLine().split(",");
			lines.add(line[2] + "," + line[3] + "," + line[4] + "," + line[5] + "," +
				line[6] + "," + line[7] + "," + line[8] + "," + line[9]);
		}
	
		br.close();
		input.close();
	
		return lines;
	}
	
	public void getHdFilesInfo(String filename) throws Exception{
		Path path = new Path(filename);
		FileStatus fStatus = hadoopFs.getFileStatus(path);
		if(fStatus.isFile()) {
			System.out.println("파일 블럭 사이즈 : " + fStatus.getBlockSize());
			System.out.println("파일 Group : " + fStatus.getGroup());
			System.out.println("파일 Owner : " + fStatus.getOwner());
			System.out.println("파일 길이 : " + fStatus.getLen());
		} else {
			System.out.println("파일이 아닙니다.!!");
		}
	
	}
	
	public void sendLines2Kafka(String topic, String line){
		System.out.println(line);
		
		// 매개변수로 입력된 문자열을 카프카의 해당 토픽으로 전송합니다.
		ProducerRecord<String, String> kafkaProducerRecord = new ProducerRecord<String, String>(topic, line);
		kafkaProducer.send(kafkaProducerRecord, new KafkaProducerCallback());
		kafkaProducer.flush();
	}
	
	public void closeStream() throws Exception {
		if(hadoopFs != null) {
			hadoopFs.close();
		}
		
		if (kafkaProducer != null) {
			kafkaProducer.close();
		}
	}
	
	//callback 인터페이스를 구현하여 카프카 프로듀서의 송신상황을 추적합니다.
	class KafkaProducerCallback implements Callback {
	
		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			// TODO Auto-generated method stub
			if(exception != null) {
				System.out.println(exception.getMessage());
			} else {
				System.out.println(metadata.topic() + "으로 " + metadata.serializedValueSize() + " 전송");
			}
		}
	}
}
