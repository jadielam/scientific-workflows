package io.biblia.workflows.utils;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import io.biblia.workflows.ConfigurationKeys;
import io.biblia.workflows.manager.action.ActionState;
import io.biblia.workflows.manager.dataset.DatasetState;
import io.biblia.workflows.definition.ActionType;
import io.biblia.workflows.Configuration;

public class MongoClientBuilder implements ConfigurationKeys {
	
	private static MongoClient instance = null;
	/**
	 * Creates a MongoClient that supports multithreading.
	 * @return
	 */
	public static MongoClient getMongoClient() {
		
		if (null == instance) {
			String mongo_host = Configuration.getValue(MONGODB_HOST, "192.168.99.100");
			int mongo_port = Integer.parseInt(Configuration.getValue(MONGODB_PORT, "27017"));
			
			CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
					CodecRegistries.fromCodecs(new ActionStateCodec(), 
							new ActionTypeCodec(),
							new DatasetStateCodec()), 
					MongoClient.getDefaultCodecRegistry());
			
			MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
			//builder.threadsAllowedToBlockForConnectionMultiplier(50000);
			builder.socketKeepAlive(true);
			//builder.connectionsPerHost(10000);
			//builder.minConnectionsPerHost(2500);
			builder.codecRegistry(codecRegistry);
			MongoClientOptions options = builder.build();
			
			instance = new MongoClient(new ServerAddress(mongo_host, mongo_port), options);
		}
		
		return instance;
	}
}

class ActionStateCodec implements Codec<ActionState> {

	@Override
	public void encode(BsonWriter writer, ActionState a, EncoderContext ec) {
		writer.writeString(a.name());
	}

	@Override
	public Class<ActionState> getEncoderClass() {
		return ActionState.class;
	}

	@Override
	public ActionState decode(BsonReader arg0, DecoderContext arg1) {
		String value = arg0.readString();
		return ActionState.valueOf(value);
	}
}

class DatasetStateCodec implements Codec<DatasetState> {

	@Override
	public void encode(BsonWriter arg0, DatasetState arg1, EncoderContext arg2) {
		arg0.writeString(arg1.name());
		
	}

	@Override
	public Class<DatasetState> getEncoderClass() {
		return DatasetState.class;
	}

	@Override
	public DatasetState decode(BsonReader arg0, DecoderContext arg1) {
		String value = arg0.readString();
		return DatasetState.valueOf(value);
	}
	
}

class ActionTypeCodec implements Codec<ActionType> {

	@Override
	public void encode(BsonWriter arg0, ActionType arg1, EncoderContext arg2) {
		arg0.writeString(arg1.name());
	}

	@Override
	public Class<ActionType> getEncoderClass() {
		return ActionType.class;
	}

	@Override
	public ActionType decode(BsonReader arg0, DecoderContext arg1) {
		String value = arg0.readString();
		return ActionType.valueOf(value);
	}
	
	
}