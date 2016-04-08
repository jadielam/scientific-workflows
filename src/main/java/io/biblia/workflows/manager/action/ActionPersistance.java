package io.biblia.workflows.manager.action;

import java.util.List;

import io.biblia.workflows.definition.Action;

public interface ActionPersistance {
	
	public List<Action> getAvailableActions();
	
	public void updateActionState(Action action, ActionState state) throws OutdatedActionException;

	public void addActionSubmissionId(Action action, String submissionId) throws OutdatedActionException;
	
}

class OutdatedActionException extends Exception {
	
}
