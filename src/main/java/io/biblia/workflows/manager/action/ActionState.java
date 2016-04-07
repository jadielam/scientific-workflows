package io.biblia.workflows.manager.action;

/**
 * Explanation of the different action states:
 * 
 * READY: Means that the action is in the database and ready to be
 * submitted to Hadoop
 * 
 * PROCESSING: Means that the ActionScraper has found the action and
 * has placed it in the actionsQueue of the actions to be submitted.
 * 
 * SUBMITTED: Means that the action has been taken from the queue and has been
 * submitted to Hadoop.
 * 
 * RUNNING: Means that Hadoop is executing the action.
 * 
 * FINISHED: Means that Hadoop has finished executing the action with sucess.
 * 
 * FAILED: Means that a run time error occurred and the action did not finish
 * executing after starting to execute.
 * 
 * KILLED: Means that the user killed the action after it started executing.
 */
public enum ActionState {

	READY, PROCESSING, SUBMITTED, RUNNING, FINISHED,
	FAILED, KILLED;
}
