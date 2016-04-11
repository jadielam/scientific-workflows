package io.biblia.workflows.manager.action;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class MongoActionPersistance implements ActionPersistance {
    
    public static final String ACTIONS_COLLECTION;
    private final MongoClient mongo;
    private final MongoDatabase workflows;
    private final MongoCollection actions
    
    public MongoActionPersistance(MongoClient mongo) {
        this.client = mongo;
        this.actions = this.mongo.getDatabase(WORKFLOWS_DATABASE);
        
    }
    
    @Override
    public List<Action> getAvailableActions() {
      List<Action> toReturn = new ArrayList<Action> toReturn;
      
      return toReturn;
    }
    
    @Override
    public void updateActionState(Action action, ActionState state) throws OutdatedActionException {
    
    }
    
    @Override
    public void addActionSubmissionId(Action action, String submissionId) {
    
    }
}
