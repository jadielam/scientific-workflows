package io.biblia.workflows.manager.action;

import java.util.List;
import java.util.Date;

import io.biblia.workflows.definition.Action;

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
	 * Returns all the actions that are in the SUBMITTED state.
	 * @return
	 */
	public List<PersistedAction> getSubmittedActions();
	
	
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
	 * Retrieves a persisted action by the id used to submit to Ooze.
	 * @param submissionId
	 * @return
	 * @throws WorkflowParseException
	 * @throws NullPointerException
	 * @throws JsonParseException
	 */
	public PersistedAction getActionBySubmissionId(String submissionId) throws WorkflowParseException,
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
	public PersistedAction addStartAndEndTimeAndSize(PersistedAction action, Date startTime, Date endTime,
			Double sizeInMB) throws OutdatedActionException, NullPointerException, JsonParseException, WorkflowParseException;
	
	/**
	 * Inserts a new action to the persistance that is in READY state
	 * (ready to be submitted to Hadoop).
	 * @param action
	 * @param parentsActionIds The ObjectIds of the parents already submitted
	 * to the system
	 * @return
	 */
	public String insertReadyAction(Action action, Long workflowId, List<String> parentsActionIds, List<String> parentActionOutputs);
	
	/**
	 * Inserts a new action to the persistance that is in WAITING state.
	 * (waiting for parent actions to finish).
	 * @param action
	 * @param parentsActionIds The ObjectIds of the parents already submitted to
	 * MongoDB
	 * @return
	 */
	public String insertWaitingAction(Action action, Long workflowId, List<String> parentsActionIds, List<String> parentActionOutputs);
	
	/**
	 * Inserts a new action to the persistance that is in the COMPUTED state. 
	 * That means that the action was computed in a previous workflow submission
	 * and does not need to be computed now.  This is inserted mainly to keep good
	 * accounting of actions.
	 * @param action
	 * @param workflowId
	 * @param parentsActionIds
	 * @return
	 */
	public String insertComputedAction(Action action, Long workflowId, List<String> parentsActionIds, List<String> parentActionOutputs);
	
	/**
	 * Updates the state of an action ignoring the version of the action.
	 * @param id
	 * @param state
	 */
	public void forceUpdateActionState(ObjectId id, ActionState state);
	
	/**
	 * FOrce updates the action state to FINISHED, as well as it adds
	 * a counter value to it.
	 * @param id the database id of the action
	 */
	public void actionFinished(ObjectId id);
	
	/**
	 * Force updates the action state to FAILED, as well as it adds a counter
	 * value to it
	 * @param id the database id of the action.
	 */
	public void actionFailed(ObjectId id);
	
	/**
	 * Forces update the action state to KILLED, as well as it adds a counter
	 * value to it.
	 * @param id
	 */
	public void actionKilled(ObjectId id);
	
	/**
	 * The implementation of this function is interesting:
	 * 1. It finds all the child actions of actionId
	 * 2. It removes actionId from all those child actions
	 * 3. from the list found in 1, it marks as READY all the actions
	 * that do not have any other dependency.
	 * 
	 * @param actionId
	 * @return It returns all the child actions found on step 1.
	 */
	public List<String> readyChildActions(String actionId);
	
	/**
	 * If the action state is WAITING, and if it has no parents,
	 * for which it is waiting, it changes its state to READY.
	 * @param actionId
	 */
	public void readyAction(String actionId);
	
	/**
	 * Adds dependecy to action represented by childDatabaseId on action represented by
	 * parentDatabaseId.  Dependency means that child action needs to wait for parent action
	 * before it can start with its own computation.
	 * 
	 * If the child action does not exist, throw exception
	 * If the parent action does not exist, throw exception
	 * @param childId
	 * @param databaseId
	 */
	public void addParentIdToAction(String childDatabaseId, String parentDatabaseId);
	
	/**
	 * Returns the next id that a workflow submitted to the system should have.
	 * It uses mongodb internal counters to determine the id.
	 * @return
	 */
	public Long getNextWorkflowSequence();
	
	
}

class OutdatedActionException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4339790589818686835L;
	
}
