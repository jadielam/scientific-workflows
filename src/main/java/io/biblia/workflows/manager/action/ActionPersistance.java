package io.biblia.workflows.manager.action;

import io.biblia.workflows.definition.Action;

import java.util.List;

public interface ActionPersistance {
	
	public List<PersistedAction> getAvailableActions();
	
	public void updateActionState(Action action, ActionState state) throws OutdatedActionException;

	public void addActionSubmissionId(Action action, String submissionId) throws OutdatedActionException;
	
}

class OutdatedActionException extends Exception {
	
}
