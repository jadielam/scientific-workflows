package io.biblia.workflows.manager.action;

import java.util.List;

public interface ActionPersistance {
	
	/**
	 * Returns all the actions that are in the READY state,
	 * or that are in the PROCESSING state but have been there
	 * for a long time.
	 * @param n if n is -1 it returns all the actions, otherwise,
	 * it returns n of the actions.
	 * @return
	 */
	public List<PersistedAction> getAvailableActions(int n);
	
	/**
	 * Updates the state of the action to the specified state.
	 * If the action has been modified by someone else, it throws
	 * OutdatedActionException
	 * @param action
	 * @param state
	 * @throws OutdatedActionException
	 */
	public void updateActionState(PersistedAction action, ActionState state) throws OutdatedActionException;
	
	/**
	 * Adds the Oozie submission id to the action.  If the action has
	 * been modified by someone else, it throws the OutdatedActionException.
	 * @param action
	 * @param id
	 * @throws OutdatedActionException
	 */
	public void addActionSubmissionId(PersistedAction action, String id) throws OutdatedActionException;
}

class OutdatedActionException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4339790589818686835L;
	
}
