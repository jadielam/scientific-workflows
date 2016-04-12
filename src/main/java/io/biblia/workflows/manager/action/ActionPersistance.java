package io.biblia.workflows.manager.action;

import java.util.List;

public interface ActionPersistance {
	
	public List<PersistedAction> getAvailableActions();
	
	public void updateActionState(PersistedAction action, ActionState state) throws OutdatedActionException;
	
}

class OutdatedActionException extends Exception {
	
}
