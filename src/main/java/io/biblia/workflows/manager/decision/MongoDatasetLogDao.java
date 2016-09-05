package io.biblia.workflows.manager.decision;

import io.biblia.workflows.manager.DatabaseConstants;

import com.google.common.base.Preconditions;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;


import java.util.Date;
import org.bson.Document;

import io.biblia.workflows.manager.dataset.DatasetState;

import static com.mongodb.client.model.Filters.*;

//TODO: Create mongo index for marker field in datasets_log entries.
public class MongoDatasetLogDao implements DatasetLogDao, DatabaseConstants {

	private final MongoClient mongo;
	private final MongoDatabase workflows;
	private final MongoCollection<Document> datasets_log;
	private final MongoCollection<Document> counters;
	private static MongoDatasetLogDao instance = null;
	
	private Long marker = null;
	
	private long storageUsedAtMarker = -1;
	
	private MongoDatasetLogDao(MongoClient mongo) {
		this.mongo = mongo;
		this.workflows = this.mongo.getDatabase(WORKFLOWS_DATABASE);
		this.datasets_log = this.workflows.getCollection(DATASETS_LOG_COLLECTION);
		this.counters = this.workflows.getCollection(COUNTERS_COLLECTION);
	}
	
	@Override
	public String insertLogEntry(String datasetPath, DatasetState previousState, 
			DatasetState newState, Double sizeInMB) {
		Preconditions.checkNotNull(datasetPath,
				"datasetPath cannot be null");
		Preconditions.checkNotNull(previousState,
				"previousState cannot be null");
		Preconditions.checkNotNull(newState,
				"newState cannot be null");
		Preconditions.checkNotNull(sizeInMB, 
				"sizeInMB cannot be null");
		Preconditions.checkArgument(sizeInMB.longValue() >= 0,
				"The sizeInMB most be greater than or equal to zero.");
		Preconditions.checkArgument(!previousState.equals(newState), 
				"The previousState cannot be equal to newState");
		
		Document insert = new Document().append("datasetPath", datasetPath)
												.append("previousState", previousState)
												.append("newState", newState)
												.append("marker", this.getNextLogSequence())
												.append("sizeInMB", sizeInMB)
												.append("lastUpdatedDate", new Date());
		
		this.datasets_log.insertOne(insert);
		return insert.getObjectId("_id").toHexString();
	}

	@Override
	public long currentlyUsedSpace() {
		
		FindIterable<Document> documents;
		if (null == this.marker || -1 == this.storageUsedAtMarker) {
			documents = this.datasets_log.find();
		}
		else {
			documents = this.datasets_log.find(
					gt("marker", this.marker)
				);
		}
		
		MongoCursor<Document> iterator = documents.iterator();
		try {
			while (iterator.hasNext()) {
				Document next = iterator.next();
				long size = next.getLong("sizeInMB");
				Long newMarker = next.getLong("marker");
				String previousStateString = next.getString("previousState");
				DatasetState previousState = DatasetState.valueOf(previousStateString);
				String newStateString = next.getString("newState");
				DatasetState newState = DatasetState.valueOf(newStateString);
				
				if (DatasetState.DELETED.equals(newState)) {
					this.storageUsedAtMarker -= size;
				}
				else if (!DatasetState.STORED.equals(previousState) &&
						 !DatasetState.LEAF.equals(previousState) &&
						 !DatasetState.STORED_TO_DELETE.equals(previousState)) {
					
					if (DatasetState.STORED.equals(newState) ||
						DatasetState.LEAF.equals(newState) ||
						DatasetState.STORED_TO_DELETE.equals(newState))
						{
							this.storageUsedAtMarker += size;
						}
				
				}
				this.marker = newMarker;
			}
		}
		
		finally {
			iterator.close();			
		}
		
		return this.storageUsedAtMarker;
	}
	
	private Long getNextLogSequence() {
		final Document filter = new Document().append("_id", "datasets_log");
		final Document update = new Document().append("$inc", new Document("seq", 1));
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
		options.returnDocument(ReturnDocument.AFTER);
		options.upsert(true);
		Document newDocument = this.counters.findOneAndUpdate(filter, update, options);
		Long toReturn = newDocument.getLong("seq");
		return toReturn;
 	}
	
	public static DatasetLogDao getInstance(MongoClient mongo) {
		if (null == instance) {
			instance = new MongoDatasetLogDao(mongo);
		}
		return instance;
	}

}
