package io.biblia.workflows.manager.action;

import org.bson.types.ObjectId;
import io.biblia.workflows.manager.dataset.DatasetPersistance;

import com.google.common.base.Preconditions;

/**
 * Handles the changing of an action to a state
 * @author jadiel
 *
 */
public class Callback {

	//TODO: Add logging of errors if there is an error talking to the database
	//here. It is critical, since the system can enter an infinite loop of actions
	//being submitted if it fails to talk to database.
	

	private final ActionPersistance aPersistance;
	private final DatasetPersistance dPersistance;
	
	public Callback(ActionPersistance aPersistance,
			DatasetPersistance dPersistance) {
		Preconditions.checkNotNull(aPersistance);
		Preconditions.checkNotNull(dPersistance);
		this.aPersistance = aPersistance;
		this.dPersistance = dPersistance;
	}
	
	public void actionFinished(String actionId) {
		decreaseDatasetClaims(actionId);
		readyChildActions(actionId);
		ObjectId id = new ObjectId(actionId); 
		this.aPersistance.forceUpdateActionState(id, ActionState.FINISHED);
	}
	
	public void actionFailed(String actionId) {
		decreaseDatasetClaims(actionId);
		ObjectId id = new ObjectId(actionId);
		this.aPersistance.forceUpdateActionState(id, ActionState.FAILED);
	}
	
	public void actionKilled(String actionId) {
		decreaseDatasetClaims(actionId);
		ObjectId id = new ObjectId(actionId);
		this.aPersistance.forceUpdateActionState(id, ActionState.KILLED);
	}
	
	/**
	 * Reduce by one the counter on each dataset on which I have layed a claim
	 * before.
	 * @param actionId
	 */
	private void decreaseDatasetClaims(String actionId) {
		//TODO
		//TODO: Add decreasing the claims on datasets on which I hold a claim.  
		//I will know that by getting the datasets ids from the database.
	}
	
	/**
	 * If a child action has all of its parents finished, change its
	 * state to READY.
	 * @param actionId
	 */
	private void readyChildActions(String actionId) {
		//TODO
		//In order to do this, I need to be sure that I am storing the actions
		//of the workflow in the correct format.
	}
	
}
