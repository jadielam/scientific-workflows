package io.biblia.workflows.statistics;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.google.common.base.Preconditions;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.Dataset;
import io.biblia.workflows.definition.Workflow;

public class WorkflowsMongoDAO implements DatabaseConstants, WorkflowsDAO{
	
	private static WorkflowsMongoDAO instance;
	private final MongoClient mongoClient;
	private final MongoDatabase database;
	
	private WorkflowsMongoDAO(MongoClient mongoClient) {
		Preconditions.checkNotNull(mongoClient);
		this.mongoClient = mongoClient;
		this.database = this.mongoClient.getDatabase(DATABASE_NAME);	
	}
	
	public ObjectId addWorkflow(Workflow workflow) {
		Document document = this.convertToDocument(workflow);
		return insertDocumentToCollection(document, WORKFLOWS_COLLECTION);
	}
		
	public ObjectId addAction(Action action) {
		Document document = this.convertToDocument(action);
		return insertDocumentToCollection(document, ACTIONS_COLLECTION);
	}
	
	public ObjectId addSavedDataset(Dataset dataset) {
		Document document = this.convertToDocument(dataset);
		return insertDocumentToCollection(document, DATASETS_COLLECTION);
	}
	
	public void addExecutionTimeToAction(ObjectId actionId, long milliseconds) {
		Document filterQuery = new Document("_id", actionId);
		Document updateQuery = new Document("$set", new Document("executionTime", milliseconds));
		this.updateDocumentInCollection(filterQuery, updateQuery, ACTIONS_COLLECTION);
	}
	
	public void addStorageSpaceToDataset(String datasetPath, long megabytes) {
		Document filterQuery = new Document("_id", datasetPath);
		Document updateQuery = new Document("$set", new Document("storageSpace", megabytes));
		
		this.updateDocumentInCollection(filterQuery, updateQuery, DATASETS_COLLECTION);
 	}
	
	public static WorkflowsMongoDAO getInstance(MongoClient mongoClient) {
		if (null == instance) {
			instance = new WorkflowsMongoDAO(mongoClient);
		}
		return instance;
	}
	
	private Document convertToDocument(Workflow workflow) {
		//TODO
		return null;
	}
	
	private Document convertToDocument(Action action) {
		//TODO
		return null;
	}

	private Document convertToDocument(Dataset dataset) {
		//TODO
		return null;
	}
	
	private ObjectId insertDocumentToCollection(Document document, String collectionName) {
		MongoCollection<Document> collection = this.database.getCollection(collectionName);
		
		//Insert standard fields here.
		Document toInsert = document.append("$currentDate", new Document().
				append("lastModified", true).
				append("insertionDate", true));
		collection.insertOne(toInsert);
		return toInsert.getObjectId("_id");
	}
	
	private UpdateResult updateDocumentInCollection(Document updateFilter, 
			Document updateAction, String collectionName) {
		MongoCollection<Document> collection = this.database.getCollection(collectionName);
		
		updateAction = updateAction.append("$currentDate", new Document()
				.append("lastModified", true));
		UpdateResult result = collection.updateOne(updateFilter, updateAction);
		return result;
	}
}
