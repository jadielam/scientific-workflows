package io.biblia.workflows.manager.action;

import org.bson.types.ObjectId;

/**
 * Handles the changing of an action to a state
 * @author jadiel
 *
 */
public class Callback {

	//TODO: Add logging of errors if there is an error talking to the database
	//here. It is critical, since the system can enter an infinite loop of actions
	//being submitted if it fails to talk to database.
	private final ActionPersistance persistance;
	
	public Callback(ActionPersistance persistance) {
		this.persistance = persistance;
	}
	
	public void actionFinished(String actionId) {
		ObjectId id = new ObjectId(actionId); 
		this.persistance.forceUpdateActionState(id, ActionState.FINISHED);
	}
	
	public void actionFailed(String actionId) {
		ObjectId id = new ObjectId(actionId);
		this.persistance.forceUpdateActionState(id, ActionState.FAILED);
	}
	
	public void actionKilled(String actionId) {
		ObjectId id = new ObjectId(actionId);
		this.persistance.forceUpdateActionState(id, ActionState.KILLED);
	}
	
}
