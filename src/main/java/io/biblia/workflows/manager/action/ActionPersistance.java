package io.biblia.workflows.manager.action;

import java.util.List;

public interface ActionPersistance {

	
	public List<ProcessingAction> getAvailableActions();
	
	public void updateProcessingAction(ProcessingAction action) throws OutdatedActionException; 
	
}

class OutdatedActionException extends Exception {
	
}
