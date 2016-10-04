package io.biblia.workflows.manager.action;

import com.google.common.base.Preconditions;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.parser.WorkflowParseException;
import io.biblia.workflows.manager.DatabaseConstants;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonParseException;
import org.bson.types.ObjectId;


import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.*;

/**
 * The Action class will be stored as the following on MongoDB:
 * {
 *     name: [String],
 *     id: [String],
 *     action: {
 *          actionFolder: [String],
 *          forceComputation: [Boolean],
 *          type: "command-line" | "map-reduce-1",
 *     }
 *     lastUpdatedDate: [Timestamp],
 *     submissionId: [String] //Submission id given by Oozie.
 *     state: ["READY", "PROCESSING", "SUBMITTED", "RUNNING", "FINISHED", "FAILED", "KILLED"];
 *     ...// Here go fields that are particular to specific action types
 *     ...//
 * }
 */
public class MongoActionPersistance implements ActionPersistance, DatabaseConstants {
    
    private final MongoClient mongo;
    private final MongoDatabase workflows;
    private final MongoCollection<Document> actions;
    private final MongoCollection<Document> counters;
    
 
    
    public MongoActionPersistance(MongoClient mongo) {
        this.mongo = mongo;
        this.workflows = this.mongo.getDatabase(WORKFLOWS_DATABASE);
        this.actions = this.workflows.getCollection(ACTIONS_COLLECTION);
        this.counters = this.workflows.getCollection(COUNTERS_COLLECTION);
     
    }
    
    @Override
    public List<PersistedAction> getSubmittedActions() {
    	List<PersistedAction> toReturn = new ArrayList<>();
    	
    	final FindIterable<Document> documents = this.actions.find(
    			eq("state", ActionState.SUBMITTED.name())
    			);
    	
    	MongoCursor<Document> iterator = documents.iterator();
    	try {
    		while(iterator.hasNext()) {
    			Document next = iterator.next();
    			try {
    				PersistedAction action = PersistedAction.parseAction(next);
    				toReturn.add(action);
    			}
    			catch(Exception e) {
                    e.printStackTrace();
                    continue;
                }
    		}
    	}
    	finally {
    		iterator.close();
    	}
    	
    	return toReturn;
    }
    
    @Override
    public List<PersistedAction> getAvailableActions(int n) {
    	List<PersistedAction> toReturn = new ArrayList<>();
        Calendar calendar = Calendar.getInstance();
        //TODO: Check that this is valid. That this is subtracting
        //seconds from the date as I envision.
        calendar.add(Calendar.SECOND, -1 * OUTDATED_SECONDS);
        Date minus = calendar.getTime();

        final FindIterable<Document> documents = this.actions.find(or(
                eq("state", ActionState.READY.name()),
                and(
                    eq("state", ActionState.PROCESSING.name()),
                    gte("lastUpdatedDate", minus)
                )
            )
        );
        
        if (n > 0) {
        	documents.limit(n);
        }
        
        MongoCursor<Document> iterator = documents.iterator();
        try {
            while (iterator.hasNext()) {
                Document next = iterator.next();
                try{
                    PersistedAction action = PersistedAction.parseAction(next);
                    toReturn.add(action);
                }
                catch(Exception e) {
                    System.out.println("Error parsing action");
                    e.printStackTrace();
                    continue;
                }
            }
        }
        finally {
            iterator.close();
        }
        return toReturn;
    }


	@Override
	public String insertReadyAction(Action action, Long workflowId, List<String> parentsActionIds, List<String> parentActionOutputs) {
		Document actionDoc = action.toBson();
		Document toInsert = new Document();
		toInsert.append("version", 1);
		toInsert.append("lastUpdatedDate", new Date());
		toInsert.append("state", ActionState.READY);
		toInsert.append("workflowId", workflowId);
		toInsert.append("action", actionDoc);
		toInsert.append("parentsActionIds", parentsActionIds);
		toInsert.append("parentActionOutputs", parentActionOutputs);
		this.actions.insertOne(toInsert);
		return toInsert.getObjectId("_id").toHexString();
	}
	
	@Override
	public String insertWaitingAction(Action action, Long workflowId, List<String> parentsActionIds, List<String> parentActionOutputs) {
		Document actionDoc = action.toBson();
		Document toInsert = new Document();
		toInsert.append("version", 1);
		toInsert.append("lastUpdatedDate", new Date());
		toInsert.append("workflowId", workflowId);
		toInsert.append("state", ActionState.WAITING);
		toInsert.append("action", actionDoc);
		toInsert.append("parentsActionIds", parentsActionIds);
		toInsert.append("parentActionOutputs", parentActionOutputs);
		this.actions.insertOne(toInsert);
		return toInsert.getObjectId("_id").toHexString();
	}
	
	@Override
	public String insertComputedAction(Action action, Long workflowId, List<String> parentsActionIds, List<String> parentActionOutputs) {
		Document actionDoc = action.toBson();
		Document toInsert = new Document();
		toInsert.append("version", 1);
		toInsert.append("lastUpdatedDate", new Date());
		toInsert.append("workflowId", workflowId);
		toInsert.append("state", ActionState.COMPUTED);
		toInsert.append("action", actionDoc);
		toInsert.append("parentsActionIds", parentsActionIds);
		toInsert.append("parentActionOutputs", parentActionOutputs);
		this.actions.insertOne(toInsert);
		return toInsert.getObjectId("_id").toHexString();

	}
	
	
    /**
     * Updates action state and updates the version of the document. 
     * Returns the updated document.
     * If the document version do not coincide, it throws an OutdatedActionException
     * @throws WorkflowParseException 
     * @throws JsonParseException 
     * @throws NullPointerException 
     * @throws OutdatedActionException
     */
    @Override
    public PersistedAction updateActionState(PersistedAction action, ActionState state)
            throws OutdatedActionException, NullPointerException, JsonParseException, WorkflowParseException {
        final Document filter = new Document().append("_id", action.getId())
                .append("version", action.getVersion());
        final Document update = new Document().append("$set", new Document("state", state.name()))
        		.append("$currentDate", new Document("lastUpdatedDate", true))
        		.append("$inc", new Document("version", 1));

        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.returnDocument(ReturnDocument.AFTER);
        Document newDocument = this.actions.findOneAndUpdate(filter, update, options);
        if (null == newDocument) {
        	throw new OutdatedActionException();
        }
        else {
        	return PersistedAction.parseAction(newDocument);
        }
    }
    
    /**
     * Adds the Oozie submission id to the persisted action and updates
     * the version of the document.
     * If the version of the action and the version in the database
     * do not coincide, it throws an OutdatedActionException.
     * @throws WorkflowParseException 
     * @throws JsonParseException 
     * @throws NullPointerException 
     */
	@Override
	public PersistedAction addActionSubmissionId(PersistedAction action, String id) throws OutdatedActionException, NullPointerException, JsonParseException, WorkflowParseException {
		final Document filter = new Document().append("_id", action.getId())
				.append("version", action.getVersion());
		final Document update = new Document().append("$set", new Document("submissionId", id))
        		.append("$currentDate", new Document("lastUpdatedDate", true))
        		.append("$inc", new Document("version", 1));
		
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
		options.returnDocument(ReturnDocument.AFTER);
		Document newDocument = this.actions.findOneAndUpdate(filter, update, options);
		if (null == newDocument) {
			throw new OutdatedActionException();
		}
		else {
			return PersistedAction.parseAction(newDocument);
		}
	}
	
	@Override
	public PersistedAction addStartAndEndTimeAndSize(PersistedAction action, Date startTime, Date endTime,
			Double sizeInMB)
			throws OutdatedActionException, NullPointerException, JsonParseException, WorkflowParseException{
		
		Preconditions.checkNotNull(action);
		Preconditions.checkNotNull(startTime);
		Preconditions.checkNotNull(endTime);
		final Document filter = new Document().append("_id", action.getId())
				.append("version", action.getVersion());
		final Document update = new Document().append("$set", new Document("startTime", startTime))
				.append("$set", new Document("endTime", endTime))
				.append("$set", new Document("sizeInMB", sizeInMB))
				.append("$currentDate", new Document("lastUpdatedDate", true))
				.append("$inc", new Document("version", 1));
		
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
		options.returnDocument(ReturnDocument.AFTER);
		Document newDocument = this.actions.findOneAndUpdate(filter,  update, options);
		if (null == newDocument) {
			throw new OutdatedActionException();
		}
		else {
			return PersistedAction.parseAction(newDocument);
		}
	}

	@Override
	public void forceUpdateActionState(ObjectId id, ActionState state) {
		
		final Document filter = new Document().append("_id", id);
		final Document update = new Document().append("$set", new Document("state", state))
				.append("$currentDate", new Document("lastUpdatedDate", true))
				.append("$inc", new Document("version", 1));
		this.actions.updateOne(filter, update);
	}

	@Override
	public PersistedAction getActionById(String actionId) throws WorkflowParseException,
		NullPointerException, JsonParseException
	{
		ObjectId id = new ObjectId(actionId);
		final Document filter = new Document().append("_id", id);
		final Document update = new Document();
		final Document found = this.actions.findOneAndUpdate(filter, update);
		if (null != found) {
			PersistedAction toReturn = PersistedAction.parseAction(found);
			return toReturn;
		}
		return null;
		
	}
	
	@Override
	public PersistedAction getActionBySubmissionId(String submissionId) throws WorkflowParseException,
		NullPointerException, JsonParseException {
		
		final Document filter = new Document().append("submissionId", submissionId);
		final Document update = new Document();
		final Document found = this.actions.findOneAndUpdate(filter, update);
		if (null != found) {
			PersistedAction toReturn = PersistedAction.parseAction(found);
			return toReturn;
		}
		return null;
	}

	@Override
	public void readyAction(String databaseId) {
		ObjectId actionId = new ObjectId(databaseId);
		final Bson readyFilter = and(
        		eq("state", ActionState.WAITING.name()),
        		size("parentsActionIds", 0),
        		eq("_id", actionId)
        	);
        final Document readyUpdate = new Document().append("$set", new Document("state", ActionState.READY.name()))
				.append("$currentDate", new Document("lastUpdatedDate", true))
				.append("$inc", new Document("version", 1));
        this.actions.updateOne(readyFilter, readyUpdate);
		
	}
	
	@Override
	public List<String> readyChildActions(String actionId) {
		//1. Find all the child actions with actionId as parent
		List<PersistedAction> childActions = new ArrayList<>();
		Bson filter = and(
                eq("state", ActionState.WAITING.name()),
                //TODO: CHeck that this is good here with elemMatch
                elemMatch("parentsActionIds", eq("parentsActionIds", actionId))
            );
		final FindIterable<Document> documents = this.actions.find(filter);
        
		MongoCursor<Document> iterator = documents.iterator();
        try {
            while (iterator.hasNext()) {
                Document next = iterator.next();
                try{
                    PersistedAction action = PersistedAction.parseAction(next);
                    childActions.add(action);
                }
                catch(Exception e) {
                    //TODO: Add logging here.
                	e.printStackTrace();
                    continue;
                }
            }
        }
        finally {
            iterator.close();
        }
        List<String> childIds = new ArrayList<String>();
        for (PersistedAction action : childActions) {
        	String childId = action.get_id().toHexString();
        	childIds.add(childId);
        }
		
		//2. Remove actionId from all the child actions that have it as parent
        Document update = new Document()
        		.append("$pull", new Document("parentsActionIds", actionId));
        this.actions.updateMany(filter, update);
		
		//3. Mark as READY all actions that are not ready and that are among 
		//the actions in list 1, and whose list of parent actions were emptied by
		//step 2.
        final Bson readyFilter = and(
        		eq("state", ActionState.WAITING.name()),
        		size("parentsActionIds", 0),
        		in("_id", childIds)
        	);
        final Document readyUpdate = new Document().append("$set", new Document("state", ActionState.READY.name()))
				.append("$currentDate", new Document("lastUpdatedDate", true))
				.append("$inc", new Document("version", 1));
        this.actions.updateMany(readyFilter, readyUpdate);
        return childIds;
		
	}

	@Override
	public void addParentIdToAction(String childDatabaseId, String parentDatabaseId) {
		
		final Document filter = new Document().append("_id", new ObjectId(childDatabaseId));
        
		final Document update = new Document().append("$push", new Document("parentsActionIds", parentDatabaseId));
        
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.returnDocument(ReturnDocument.AFTER);
        this.actions.findOneAndUpdate(filter, update, options);
	}

	@Override
	public void actionFinished(ObjectId id) {
		final Document filter = new Document().append("_id", id);
		final Document update = new Document().append("$set", new Document("state", ActionState.FINISHED))
				.append("$set", new Document("marker", getNextLogSequence()))
				.append("$currentDate", new Document("lastUpdatedDate", true))
				.append("$inc", new Document("version", 1));
		this.actions.updateOne(filter, update);
	}

	@Override
	public void actionFailed(ObjectId id) {
		final Document filter = new Document().append("_id", id);
		final Document update = new Document().append("$set", new Document("state", ActionState.FAILED))
				.append("$set", new Document("marker", getNextLogSequence()))
				.append("$currentDate", new Document("lastUpdatedDate", true))
				.append("$inc", new Document("version", 1));
		this.actions.updateOne(filter, update);
	}

	@Override
	public void actionKilled(ObjectId id) {
		final Document filter = new Document().append("_id", id);
		final Document update = new Document().append("$set", new Document("state", ActionState.KILLED))
				.append("$set", new Document("marker", getNextLogSequence()))
				.append("$currentDate", new Document("lastUpdatedDate", true))
				.append("$inc", new Document("version", 1));
		this.actions.updateOne(filter, update);
	}

	private Long getNextLogSequence() {
		final Document filter = new Document().append("_id", "actions");
		final Document update = new Document().append("$inc", new Document("seq", 1));
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
		options.returnDocument(ReturnDocument.AFTER);
		options.upsert(true);
		Document newDocument = this.counters.findOneAndUpdate(filter,  update, options);
		Long toReturn = new Long(newDocument.getInteger("seq"));
		return toReturn;
	}
	
	@Override
	public Long getNextWorkflowSequence() {
		final Document filter = new Document().append("_id", "workflows");
		final Document update = new Document().append("$inc", new Document("seq", 1L));
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
		options.returnDocument(ReturnDocument.AFTER);
		options.upsert(true);
		Document newDocument = this.counters.findOneAndUpdate(filter, update, options);
		Long toReturn = newDocument.getLong("seq");
		return toReturn;
	}




}
