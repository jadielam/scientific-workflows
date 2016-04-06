package io.biblia.workflows.manager.action;

import java.util.List;

import io.biblia.workflows.definition.Action;

public interface ActionPersistance {
	
	public List<Action> getAvailableActions();
	
	public void updateProcessingAction(Action action) throws OutdatedActionException; 
	
}

class OutdatedActionException extends Exception {
	
}
