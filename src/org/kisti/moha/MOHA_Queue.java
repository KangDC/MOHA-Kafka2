package org.kisti.moha;

import java.util.Collections;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import kafka.utils.ZkUtils$;

public class MOHA_Queue {

	private String queueName;
	private KafkaProducer<Integer, String> producer;
	private KafkaConsumer<Integer, String> consumer;
	private ZkClient zkClient;
	private String zookeeperConnect;
	private int sessionTimeout;
	private int connectionTimeout;
	private ZkConnection zkConnection;
	private ZkUtils zkUtils;

	public MOHA_Queue(String queueName) {
		// TODO Auto-generated constructor stub
		this.queueName = queueName;
	}

	// create queue
	public boolean create(int numPartitions, int numReplicationFactor) {
		// TODO Auto-generated method stub
		zkClient = ZkUtils$.MODULE$.createZkClient(zookeeperConnect, sessionTimeout, connectionTimeout);
		zkConnection = new ZkConnection(zookeeperConnect, sessionTimeout);
		zkUtils = new ZkUtils(zkClient, zkConnection, false);
		AdminUtils.createTopic(zkUtils, queueName, numPartitions, numReplicationFactor, new Properties(), null);
		return true;	
	}
	
	// delete queue
	public boolean deleteQueue() {
		AdminUtils.deleteTopic(zkUtils, queueName);
		return true;
	}
	

	// register queue
	public boolean register() {
		// TODO Auto-generated method stub
		Properties props = new Properties();
		//props.put("bootstrap.servers", "hdp01.kisti.re.kr:9092, hdp02.kisti.re.kr:9092, hdp03.kisti.re.kr:9092");
		props.put("bootstrap.servers", "localhost:9092");
		//props.put("client.id", queueName);
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<Integer, String>(props);
		return true;
	}
	
	//unregister queue
	public boolean unregister() {
		producer.close();
		return true;
	}
	
	public boolean commitSync(){
		consumer.commitSync();
		return true;
	}
	
	// Subscribe to poll messages from queue
	public boolean subcribe() {
		Properties props = new Properties();
		
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, queueName);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put("request.timeout.ms", "40000");
		//props.put("receive.buffer.bytes", "1024");
		//props.put("max.partition.fetch.bytes", "1048576");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(queueName));
		return true;
	}
	
	public boolean close() {
		consumer.close();
		return true;
	}

	public String push(String messages) {
		// TODO Auto-generated method stub
		producer.send(new ProducerRecord<Integer, String>(queueName, 0, messages));
		return messages;
	}

	public String push(int key, String messages) {
		// TODO Auto-generated method stub
		producer.send(new ProducerRecord<Integer, String>(queueName, key, messages));
		return messages;
	}
	
	public ConsumerRecords<Integer, String> poll(int timeOut) {
		ConsumerRecords<Integer, String> records = consumer.poll(timeOut);
		return records;
	}

}
