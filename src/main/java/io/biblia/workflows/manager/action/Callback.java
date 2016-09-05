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
		this.aPersistance.actionFinished(id);
		
		//4.1 Updating start and end time of the action.
		//4.2 Insert a record of the output of the action into the database
		try{
			//1. Updating start and end time.
			PersistedAction action = this.aPersistance.getActionById(actionId);
			String oozieId = action.getSubmissionId();
			List<Date> times = OozieClientUtil.getStartAndEndTime(oozieId);
			Date startTime = times.get(0);
			Date endTime = times.get(1);
			//2. Inserting a record of the output action into the database.
			String outputPath = action.getAction().getOutputPath();
			Double sizeInMB = HdfsUtil.getSizeInMB(outputPath);
			
			this.aPersistance.addStartAndEndTimeAndSize(action, startTime, endTime, sizeInMB);
			
			
			if (null != sizeInMB) {
				PersistedDataset actionDataset = this.dPersistance.getDatasetByPath(outputPath);
				if (null == actionDataset) {
					PersistedDataset newDataset = new PersistedDataset(outputPath,
							sizeInMB, DatasetState.STORED, new Date(), 1, childActionIds);
					this.dPersistance.insertDataset(newDataset);
				}
				else {
					DatasetState state = actionDataset.getState();
					if (state.equals(DatasetState.TO_STORE)) {
						actionDataset = this.dPersistance.updateDatasetState(actionDataset, DatasetState.STORED);
						
					}
					else if (DatasetState.TO_LEAF.equals(state)) {
						actionDataset = this.dPersistance.updateDatasetState(actionDataset, DatasetState.LEAF);
						
					}
					else if (state.equals(DatasetState.TO_DELETE)){
						actionDataset = this.dPersistance.updateDatasetState(actionDataset, DatasetState.STORED_TO_DELETE);
						
					}
					this.dPersistance.updateDatasetSizeInMB(actionDataset, sizeInMB);
					for (String childActionId : childActionIds) {
						this.dPersistance.addClaimToDataset(actionDataset, childActionId);
					}
					
				}
				
			}
		}
		catch(Exception e) {
			//TODO: Log exception here.
			//Do nothing.
		}
		
	}
	
	public void actionFailed(String actionId) {
		
		//1. Decrease dataset claims
		decreaseDatasetClaims(actionId);
		ObjectId id = new ObjectId(actionId);
		
		//2. Remove the output produced by the action
		//and update the database with the output state accordingly.
		//In order to remove the output, just mark the dataset with
		//state TO_DELETE, and let the other guys handle it.
		try{
			PersistedAction action = this.aPersistance.getActionById(actionId);
			String outputPath = action.getAction().getOutputPath();
			PersistedDataset dataset = this.dPersistance.getDatasetByPath(outputPath);
			//TODO: There might be a bug here. Analyze later: If the dataset is not deleted immediately here
			//then it could be that actions with claims on it might use it even when it is in an inconsistent state.
			//No, it is not possible.  Actions get only notified of this dataset if
			//the action succeeded.  No actions can possibly have a claim on this
			//dataset unless they are having it through the action, because this
			//dataset did not exist, and no action places a clain on a non-existent 
			//dataset.
			this.dPersistance.updateDatasetState(dataset, DatasetState.STORED_TO_DELETE);
		}
		catch(Exception e) {
			//Do nothing and log.
		}
		
		//3. Update the action state to FAILED
		this.aPersistance.actionFailed(id);
	}
	
	public void actionKilled(String actionId) {
		
		//1. Decrease dataset claims
		decreaseDatasetClaims(actionId);
		
		//2. Remove the output produced by the action and
		//update the database with the output state accordingly.
		//IMPORTANT: For the sake of concurrency correctness,
		//we need to remove the output produced by the action
		//before updating the state of the action to KILLED.
		try {
			PersistedAction action = this.aPersistance.getActionById(actionId);
			String outputPath = action.getAction().getOutputPath();
			PersistedDataset dataset = this.dPersistance.getDatasetByPath(outputPath);
			this.dPersistance.updateDatasetState(dataset, DatasetState.STORED_TO_DELETE);
		}
		catch(Exception e) {
			//Do nothing and log.
		}

		//3. Update the state of the action.
		ObjectId id = new ObjectId(actionId);
		this.aPersistance.actionKilled(id);
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
