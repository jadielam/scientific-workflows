package io.biblia.workflows.manager.action;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.biblia.workflows.definition.Action;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;

/**
 * The Action class will be stored as the following on MongoDB:
 * {
 *     name: [String],
 *     id: [String],
 *     actionFolder: [String],
 *     forceComputation: [Boolean],
 *     lastUpdatedDate: [Timestamp],
 *     type: ["command-line", "map-reduce-1"]
 *     state: ["READY", "PROCESSING", "SUBMITTED", "RUNNING", "FINISHED", "FAILED", "KILLED"];
 *     ...// Here go fields that are particular to specific action types
 *     ...//
 * }
 */
public class MongoActionPersistance implements ActionPersistance {
    
    public static final String ACTIONS_COLLECTION = "actions_cl";
    public static final String WORKFLOWS_DATABASE = "workflows_db";
    private final MongoClient mongo;
    private final MongoDatabase workflows;
    private final MongoCollection<Document> actions;
    
    public MongoActionPersistance(MongoClient mongo) {
        this.mongo = mongo;
        this.workflows = this.mongo.getDatabase(WORKFLOWS_DATABASE);
        this.actions = this.workflows.getCollection(ACTIONS_COLLECTION);

    }
    
    @Override
    public List<PersistedAction> getAvailableActions() {
    	List<PersistedAction> toReturn = new ArrayList<>();
        final FindIterable<Document> documents = this.actions.find(or(
                eq("state", "READY"),
                and(
                        eq("state", "PROCESSING"),
                        gte("lastUpdatedDate", "test")
                )
                )
        );
        MongoCursor<Document> iterator = documents.iterator();
        try {
            while (iterator.hasNext()) {
                Document next = iterator.next();
                PersistedAction action = parseAction(next);
                toReturn.add(action);
            }
        }
        finally {
            iterator.close();
        }
        return toReturn;
    }
    
    @Override
    public void updateActionState(Action action, ActionState state)
            throws OutdatedActionException {
    
    }
    
    @Override
    public void addActionSubmissionId(Action action, String submissionId) {
    
    }

    private PersistedAction parseAction(Document document) {
        PersistedAction action = null;
        String type = (String) document.get("type");
        String id = (String) document.get("id");
        return action;
    }
}
