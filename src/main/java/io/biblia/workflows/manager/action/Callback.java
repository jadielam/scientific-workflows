package io.biblia.workflows.manager.action;

import java.util.Date;
import java.util.List;
import java.util.Collections;

import org.bson.types.ObjectId;

import io.biblia.workflows.hdfs.HdfsUtil;
import io.biblia.workflows.manager.dataset.DatasetPersistance;
import io.biblia.workflows.manager.dataset.PersistedDataset;
import io.biblia.workflows.oozie.OozieClientUtil;
import io.biblia.workflows.manager.dataset.DatasetState;

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
	
	/**
	 * I need to design this call in such a way that if
	 * it is called twice for one same action, nothing bad
	 * happens.
	 * @param actionId
	 */
	public void actionFinished(String actionId) {
		Preconditions.checkNotNull(actionId);
		
		//1. Decrease dataset claims
		decreaseDatasetClaims(actionId);
		
		//2. Change state of child actions to READY if they
		//are not waiting on any other dependants.
		List<String> childActionIds = readyChildActions(actionId);
		
		//3. Update action state
		ObjectId id = new ObjectId(actionId); 
		this.aPersistance.forceUpdateActionState(id, ActionState.FINISHED);
		
		//4.1 Updating start and end time of the action.
		//4.2 Insert a record of the output of the action into the database
		try{
			//1. Updating start and end time.
			PersistedAction action = this.aPersistance.getActionById(actionId);
			String oozieId = action.getSubmissionId();
			List<Date> times = OozieClientUtil.getStartAndEndTime(oozieId);
			Date startTime = times.get(0);
			Date endTime = times.get(1);
			this.aPersistance.addStartAndEndTime(action, startTime, endTime);
			
			//2. Inserting a record of the output action into the database.
			String outputPath = action.getAction().getOutputPath();
			Double sizeInMB = HdfsUtil.getSizeInMB(outputPath);
			if (null != sizeInMB) {
				PersistedDataset newDataset = new PersistedDataset(outputPath,
						sizeInMB, DatasetState.STORED, new Date(), 1, childActionIds);
				this.dPersistance.insertDataset(newDataset);
			}
		}
		catch(Exception e) {
			//Do nothing.
		}
		
	}
	
	public void actionFailed(String actionId) {
		
		//1. Decrease dataset claims
		decreaseDatasetClaims(actionId);
		ObjectId id = new ObjectId(actionId);
		
		//TODO
		//2. Remove the output produced by the action
		//and update the database with the output state accordingly.
		//IMPORTANT: For the sake of concurrency correctness,
		//we need to remove the output produced by the action
		//before updating the state of the action to FAILED.
		
		//3. Update the action state to FAILED
		this.aPersistance.forceUpdateActionState(id, ActionState.FAILED);
	}
	
	public void actionKilled(String actionId) {
		//1. Decrease dataset claims
		decreaseDatasetClaims(actionId);
		
		//TODO
		//2. Remove the output produced by the action and
		//update the database with the output state accordingly.
		//IMPORTANT: For the sake of concurrency correctness,
		//we need to remove the output produced by the action
		//before updating the state of the action to KILLED.

		//3. Update the state of the action.
		ObjectId id = new ObjectId(actionId);
		this.aPersistance.forceUpdateActionState(id, ActionState.KILLED);
	}
	
	/**
	 * Reduce by one the counter on each dataset on which I have layed a claim
	 * before.
	 * @param actionId
	 */
	private void decreaseDatasetClaims(String actionId) {
		this.dPersistance.removeClaimFromDatasets(actionId);
	}
	
	/**
	 * It finds all the child actions. It removes the current action from
	 * the parent actions on those child actions.  If a child action
	 * has all its parent actions gone, its state is marked to READY.
	 * @param actionId
	 * @return Returns a list with all the child actions ids
	 */
	private List<String> readyChildActions(String actionId) {
		return this.aPersistance.readyChildActions(actionId);
	}
	
}
