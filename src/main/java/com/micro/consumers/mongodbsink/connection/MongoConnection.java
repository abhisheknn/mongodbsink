package com.micro.consumers.mongodbsink.connection;


import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.kenai.constantine.Constant;
import com.micro.consumers.mongodbsink.common.Constants;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;

@Component
@Scope("singletone")

public class MongoConnection {

	
	private MongoClient mongo = null;
	public MongoClient getMongo() throws RuntimeException {
		if (mongo == null) {
		MongoClientOptions.Builder options = MongoClientOptions.builder()
                                                             .connectionsPerHost(4)
                                                            .maxConnectionIdleTime((60 * 1_000))
                                                         .maxConnectionLifeTime((120 * 1_000));
                                                             

			ServerAddress address= new ServerAddress(Constants.MONGODBHOST,Constants.MONGODBPORT);
			List<MongoCredential> credentials = new ArrayList<MongoCredential>();
			credentials.add(
			    MongoCredential.createScramSha1Credential(
			    		Constants.MONGODBUSERNAME,
			    		Constants.MONGODB_DATABASE,
			    		Constants.MONGODBPASSWORD.toCharArray()
			    )
			);
			try {
				mongo = new MongoClient(address,credentials);
			} catch (MongoException ex) {
				System.out.println(ex);
			} catch (Exception ex) {
				System.out.println(ex);
			}
		}

		return mongo;
	}
	
	public void close() {
		if (mongo != null) {
			try {
				mongo.close();
				mongo = null;
			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}
}
