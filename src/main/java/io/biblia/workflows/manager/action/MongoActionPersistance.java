package io.biblia.workflows.manager.action;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.biblia.workflows.definition.Action;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
        Map<String, String> map = new HashMap<String, String>();

    }
    
    @Override
    public List<Action> getAvailableActions() {
    	List<Action> toReturn = new ArrayList<Action>();
      
    	return toReturn;
    }
    
    @Override
    public void updateActionState(Action action, ActionState state) throws OutdatedActionException {
    
    }
    
    @Override
    public void addActionSubmissionId(Action action, String submissionId) {
    
    }
}
