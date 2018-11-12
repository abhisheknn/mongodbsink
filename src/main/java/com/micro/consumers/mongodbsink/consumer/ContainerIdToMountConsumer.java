package com.micro.consumers.mongodbsink.consumer;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.micro.consumers.mongodbsink.connection.MongoConnection;
import com.mongodb.BasicDBObject;
import com.mongodb.InsertOptions;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneOptions;
import com.sun.org.apache.bcel.internal.generic.DCONST;


public class ContainerIdToMountConsumer extends ConsumerThread {
	 private MongoClient client=null;  
	 private MongoDatabase database=null;
	 private MongoCollection<Document> collection=null;
	 InsertOneOptions insertOneOptions= new InsertOneOptions();
	 private Gson gson= new Gson();
	 Type  mapType= new TypeToken<Map<String,Object>>(){}.getType();
     Type listType= new TypeToken<List<Map<String,Object>>>(){}.getType();
     public ContainerIdToMountConsumer(MongoConnection connection,String brokers, String groupId, String topic) {
    	 super(brokers, groupId, topic);
    	client = connection.getMongo();
		database=client.getDatabase("dockerx");
		collection=database.getCollection("containersToMounts");
		insertOneOptions.bypassDocumentValidation(true);
     }
	
	
	 @Override
	  public void run() {
	
		 while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {

					Document document = new Document(record.key(), record.value());
					collection.insertOne(document,insertOneOptions);
					System.out.println("Receive message: " + record.value() + ", Partition: " + record.partition()
							+ ", Offset: " + record.offset() + ", by ThreadID: " + Thread.currentThread().getId());
				}
			}
	 }
}