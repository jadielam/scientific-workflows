package io.biblia.workflows.manager.action;


public class MongoActionPersistance implements ActionPersistance {

    public MongoActionPersistance() {
    
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
