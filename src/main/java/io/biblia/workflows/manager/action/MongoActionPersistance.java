package io.biblia.workflows.manager.action;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.parser.WorkflowParseException;
import io.biblia.workflows.definition.parser.v1.ActionParser;
import org.bson.Document;
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
public class MongoActionPersistance implements ActionPersistance {
    
    public static final String ACTIONS_COLLECTION = "actions_cl";
    public static final String WORKFLOWS_DATABASE = "workflows_db";

    /**
     * The number of seconds for an action in PROCESSING state to be
     * considered obsolete.
     */
    public static final int OUTDATED_SECONDS = 400;
    private final MongoClient mongo;
    private final MongoDatabase workflows;
    private final MongoCollection<Document> actions;
    private final io.biblia.workflows.definition.parser.ActionParser parser;
    
    public MongoActionPersistance(MongoClient mongo) {
        this.mongo = mongo;
        this.workflows = this.mongo.getDatabase(WORKFLOWS_DATABASE);
        this.actions = this.workflows.getCollection(ACTIONS_COLLECTION);
        this.parser = new ActionParser();
    }
    
    @Override
    public List<PersistedAction> getAvailableActions(int n) {
    	List<PersistedAction> toReturn = new ArrayList<>();
        Calendar calendar = Calendar.getInstance();
        //TODO: Check that this is valid.
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
                    PersistedAction action = parseAction(next);
                    toReturn.add(action);
                }
                catch(Exception e) {
                    //TODO: Add logging here.
                    continue;
                }
            }
        }
        finally {
            iterator.close();
        }
        return toReturn;
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
        	return parseAction(newDocument);
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
			return parseAction(newDocument);
		}
	}


    private PersistedAction parseAction(Document document) throws
            WorkflowParseException, NullPointerException, JsonParseException {

        ObjectId id = document.getObjectId("_id");
        Date date = (Date) document.getDate("lastUpdatedDate");
        String stateString = document.getString("state");
        ActionState state = ActionState.valueOf(stateString);
        Document actionDoc = (Document) document.get("action");
        Action action = this.parser.parseAction(actionDoc);
        int version = document.getInteger("version");

        return new PersistedAction(action, id, state, date, version);
    }

}
