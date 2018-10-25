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
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.sun.org.apache.bcel.internal.generic.DCONST;


public class ContainerDetailsConsumer extends ConsumerThread {
	 private MongoClient client=null;  
	 private MongoDatabase database=null;
	 private MongoCollection<Document> collection=null;
	 private Gson gson= new Gson();
	 Type  mapType= new TypeToken<Map<String,Object>>(){}.getType();
     Type listType= new TypeToken<List<Map<String,Object>>>(){}.getType();
     public ContainerDetailsConsumer(MongoConnection connection,String brokers, String groupId, String topic) {
    	 super(brokers, groupId, topic);
    	client = connection.getMongo();
		database=client.getDatabase("newdb");
		collection=database.getCollection("containers");
		
     }
	
	
	 @Override
	  public void run() {
		
	    while (true) {
	      ConsumerRecords<String, String> records = consumer.poll(100);
	      for (ConsumerRecord<String, String> record : records) {
	    	
	    	  Map<String, Object> map=gson.fromJson(record.value(), mapType);
	    	  List<Map<String,Object>> containerList=(List<Map<String,Object>>)map.get("value");
	    	  List<Document> documents= new ArrayList();
	    	  for(Map<String,Object> container:containerList) {
	    		  container.put("machineHostName", record.key());
	    	      documents.add(new Document(container));
	    	  }
	    	  collection.insertMany(documents);
	      
	        System.out.println("Receive message: " + record.value() + ", Partition: "
	            + record.partition() + ", Offset: " + record.offset() + ", by ThreadID: "
	            + Thread.currentThread().getId());
	      }
	    }
	 
	  }

}
