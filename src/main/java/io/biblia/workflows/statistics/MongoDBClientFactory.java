package io.biblia.workflows.statistics;

import com.mongodb.MongoClient;

import io.biblia.workflows.Configuration;
import io.biblia.workflows.ConfigurationKeys;

public class MongoDBClientFactory implements ConfigurationKeys {

	private static MongoClient client;
	
	public static MongoClient getInstance() {
		if (null == client) {
			String host = Configuration.getValue(MONGODB_HOST);
			String portString = Configuration.getValue(MONGODB_PORT);
			
			if (null == host) {
				host = "localhost";
			}
			if (null == portString) {
				portString = "27017";
			}
			int port;
			try {
				port = Integer.parseInt(portString);
			}
			catch(NumberFormatException ex) {
				port = 27017;
			}
			
			client = new MongoClient(host, port);
		}
		return client;
	}

}
