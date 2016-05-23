package io.biblia.workflows.manager.action;

import java.util.List;
import java.util.Date;

import io.biblia.workflows.definition.Action;

import org.bson.Document;
import org.bson.json.JsonParseException;
import org.bson.types.ObjectId;

import io.biblia.workflows.definition.parser.WorkflowParseException;

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
	 * @return returns the PersistedAction
	 * @throws WorkflowParseException 
	 * @throws JsonParseException 
	 * @throws NullPointerException 
	 */
	public PersistedAction updateActionState(PersistedAction action, ActionState state) throws OutdatedActionException, NullPointerException, JsonParseException, WorkflowParseException;
	
	/**
	 * Given the action id, it retrieves and parses the action into a PersistedAction
	 * @param actionId
	 * @return
	 */
	public PersistedAction getActionById(String actionId) throws WorkflowParseException,
		NullPointerException, JsonParseException;
	
	/**
	 * Adds the Oozie submission id to the action.  If the action has
	 * been modified by someone else, it throws the OutdatedActionException.
	 * @param action
	 * @param id
	 * @throws OutdatedActionException
	 * @return the updated persisted action.
	 * @throws WorkflowParseException 
	 * @throws JsonParseException 
	 * @throws NullPointerException 
	 */
	public PersistedAction addActionSubmissionId(PersistedAction action, String id) throws OutdatedActionException, NullPointerException, JsonParseException, WorkflowParseException;
	
	/**
	 * Adds start and end time to an action.
	 * @param action
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public PersistedAction addStartAndEndTime(PersistedAction action, Date startTime, Date endTime) throws OutdatedActionException, NullPointerException, JsonParseException, WorkflowParseException;
	
	/**
	 * Inserts a new action to the persistance that is in READY state
	 * (ready to be submitted to Hadoop).
	 * @param action
	 * @return
	 */
	public String insertReadyAction(Action action);
	
	/**
	 * Inserts a new action to the persistance that is in WAITING state.
	 * (waiting for parent actions to finish).
	 * @param action
	 * @return
	 */
	public String insertWaitingAction(Action action);
	
	/**
	 * Updates the state of an action ignoring the version of the action.
	 * @param id
	 * @param state
	 */
	public void forceUpdateActionState(ObjectId id, ActionState state);
	
}

class OutdatedActionException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4339790589818686835L;
	
}
