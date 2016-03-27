package io.biblia.workflows.statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

public class WorkflowsMongoDAO implements DatabaseConstants, WorkflowsDAO {
	
	private static WorkflowsDAO instance;
	private final MongoClient mongoClient;
	private final MongoDatabase database;
	private final ActionConverter actionConverter = new ActionConverter();
	private final WorkflowConverter workflowConverter = new WorkflowConverter();
	private final DatasetConverter datasetConverter = new DatasetConverter();
	
	private WorkflowsMongoDAO(MongoClient mongoClient) {
		Preconditions.checkNotNull(mongoClient);
		this.mongoClient = mongoClient;
		this.database = this.mongoClient.getDatabase(DATABASE_NAME);	
	}
	
	/** 
	 * (non-Javadoc)
	 * @see io.biblia.workflows.statistics.WorkflowsDAO#addWorkflow(io.biblia.workflows.definition.Workflow)
	 */
	@Override
	public String addWorkflow(Workflow workflow) {
		Document document = this.workflowConverter.convertToDocument(workflow);
		return insertDocumentToCollection(document, WORKFLOWS_COLLECTION).toHexString();
	}
		
	/** 
	 * (non-Javadoc)
	 * @see io.biblia.workflows.statistics.WorkflowsDAO#addAction(io.biblia.workflows.definition.Action)
	 */
	@Override
	public String addAction(Action action) {
		Document document = this.actionConverter.convertToDocument(action);
		return insertDocumentToCollection(document, ACTIONS_COLLECTION).toHexString();
	}
	
	/** 
	 * (non-Javadoc)
	 * @see io.biblia.workflows.statistics.WorkflowsDAO#addSavedDataset(io.biblia.workflows.definition.Dataset)
	 */
	@Override
	public String addSavedDataset(Dataset dataset) {
		Document document = this.datasetConverter.convertToDocument(dataset);
		return insertDocumentToCollection(document, DATASETS_COLLECTION).toHexString();
	}
	
	/** 
	 * (non-Javadoc)
	 * @see io.biblia.workflows.statistics.WorkflowsDAO#addExecutionTimeToAction(org.bson.types.ObjectId, long)
	 */
	@Override
	public void addExecutionTimeToAction(String actionId, long milliseconds) {
		ObjectId actionIDObject = new ObjectId(actionId);
		Document filterQuery = new Document("_id", actionIDObject);
		Document updateQuery = new Document("$set", new Document("executionTime", milliseconds));
		this.updateDocumentInCollection(filterQuery, updateQuery, ACTIONS_COLLECTION);
	}
	
	/** 
	 * (non-Javadoc)
	 * @see io.biblia.workflows.statistics.WorkflowsDAO#addStorageSpaceToDataset(java.lang.String, long)
	 */
	@Override
	public void addStorageSpaceToDataset(String datasetPath, long megabytes) {
		Document filterQuery = new Document("_id", datasetPath);
		Document updateQuery = new Document("$set", new Document("storageSpace", megabytes));
		
		this.updateDocumentInCollection(filterQuery, updateQuery, DATASETS_COLLECTION);
 	}
	
	public static WorkflowsDAO getInstance(MongoClient mongoClient) {
		if (null == instance) {
			instance = new WorkflowsMongoDAO(mongoClient);
		}
		return instance;
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
	
	private interface DocumentConverter<T> {		
		public Document convertToDocument(T t);
	}

	private class ActionConverter implements DocumentConverter<Action> {

		@Override
		public Document convertToDocument(Action a) {
			Document toReturn = new Document()
					.append("name", a.getName())
					.append("forceComputation", a.getForceComputation())
					.append("parentActionNames", a.getParentActionNames())
					.append("inputParameters", this.convertToDocumentList(a.getInputParameters()))
					.append("outputParameters", this.convertToDocumentList(a.getOutputParameters()))
					.append("configurationParameters", this.convertToDocumentList(a.getConfigurationParameters()));
			
			return toReturn;
		}
		
		private List<Document> convertToDocumentList(Map<String, String> keyValues) {
			List<Document> toReturn = new ArrayList<Document>();
			for (Entry<String, String> e : keyValues.entrySet()) {
				String key = e.getKey();
				String value = e.getValue();
				
				toReturn.add(new Document(key, value));
			}
			return toReturn;
		}
	}
	
	private class WorkflowConverter implements DocumentConverter<Workflow> {

		@Override
		public Document convertToDocument(Workflow w) {
			Document toReturn = new Document()
					.append("workflowName", w.getWorkflowName())
					.append("startActionName", w.getStartAction())
					.append("endActionName", w.getEndAction());
			
			List<Document> actions = new ArrayList<Document>();
			for (Action action : w.getActions()) {
				Document doc = actionConverter.convertToDocument(action);
				actions.add(doc);
			}
			
			toReturn.append("actions", actions);
			return toReturn;
		}	
	}
	
	private class DatasetConverter implements DocumentConverter<Dataset> {

		@Override
		public Document convertToDocument(Dataset t) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
}
