package com.micro.consumers.mongodbsink;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;

import com.micro.consumers.mongodbsink.common.Constants;
import com.micro.consumers.mongodbsink.connection.MongoConnection;
import com.micro.consumers.mongodbsink.consumer.ConsumerGroup;
import com.micro.consumers.mongodbsink.consumer.ConsumerThread;
import com.micro.consumers.mongodbsink.consumer.ContainerDetailsConsumer;
import com.micro.consumers.mongodbsink.consumer.ContainerIdToImageIdConsumer;


@SpringBootApplication
@EnableAutoConfiguration(exclude={MongoAutoConfiguration.class})
public class MongoDbSink {


	
	public static void main(String[] args) {
		SpringApplication.run(MongoDbSink.class, args);
		spwanContainerDetailsConsumer();
		spwanContainerIdToImageIdConsumer();
		spwanContainerIdToMountConsumer();
	}

	private static void spwanContainerDetailsConsumer() {
		MongoConnection connection =new MongoConnection(); 
		ConsumerThread ncThread=     new ContainerDetailsConsumer(connection,Constants.KAFKABROKER, Constants.CONTAINER_DETAILS_CONSUMER_GROUP_ID, Constants.CONTAINER_DETAILS_TOPIC);
		ConsumerGroup consumerGroup= new ConsumerGroup(ncThread, 1);
		consumerGroup.execute();
	}
	
	private static void spwanContainerIdToImageIdConsumer() {
		MongoConnection connection =new MongoConnection(); 
		ConsumerThread ncThread=     new ContainerIdToImageIdConsumer(connection,Constants.KAFKABROKER, Constants.CONTAINERID_TO_IMAGEID_CONSUMER_GROUP_ID, Constants.CONTAINERID_TO_IMAGEID_TOPIC);
		ConsumerGroup consumerGroup= new ConsumerGroup(ncThread, 1);
		consumerGroup.execute();
	}
	
	private static void spwanContainerIdToMountConsumer() {
		MongoConnection connection =new MongoConnection(); 
		ConsumerThread ncThread=     new ContainerIdToImageIdConsumer(connection,Constants.KAFKABROKER, Constants.CONTAINER_TO_MOUNT_CONSUMER_GROUP_ID, Constants.CONTAINER_TO_MOUNTS_TOPIC);
		ConsumerGroup consumerGroup= new ConsumerGroup(ncThread, 1);
		consumerGroup.execute();
	}
}
