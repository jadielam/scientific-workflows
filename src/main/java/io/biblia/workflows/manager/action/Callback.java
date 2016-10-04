package io.biblia.workflows.manager.action;

import java.util.Date;
import java.util.List;

import java.util.logging.Logger;
import java.util.logging.Level;
import org.bson.types.ObjectId;

import io.biblia.workflows.hdfs.HdfsUtil;
import io.biblia.workflows.manager.dataset.DatasetPersistance;
import io.biblia.workflows.manager.dataset.PersistedDataset;
import io.biblia.workflows.oozie.OozieClientUtil;
import io.biblia.workflows.manager.dataset.DatasetState;
import io.biblia.workflows.manager.decision.DatasetLogDao;

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
	private final DatasetLogDao dLogDao;
	final static Logger logger = Logger.getLogger(Callback.class.getName());
	
	public Callback(ActionPersistance aPersistance,
			DatasetPersistance dPersistance,
			DatasetLogDao datasetLogDao) {
		Preconditions.checkNotNull(aPersistance);
		Preconditions.checkNotNull(dPersistance);
		Preconditions.checkNotNull(datasetLogDao);
		this.aPersistance = aPersistance;
		this.dPersistance = dPersistance;
		this.dLogDao = datasetLogDao;
	}
	
	/**
	 * I need to design this call in such a way that if
	 * it is called twice for one same action, nothing bad
	 * happens.
	 * @param actionId
	 */
	public void actionFinished(PersistedAction pAction) {
		ObjectId actionId = pAction.getId();
		Preconditions.checkNotNull(actionId);
		logger.log(Level.FINE, "actionFinished called on action {0}", actionId);
		
		//1. Decrease dataset claims
		decreaseDatasetClaims(actionId.toHexString());
		
		//2. Change state of child actions to READY if they
		//are not waiting on any other dependants.
		List<String> childActionIds = readyChildActions(actionId.toHexString());
		logger.log(Level.FINE, "Readied {0} child actions of " + actionId, childActionIds.size());
		
		//4.1 Updating start and end time of the action.
		//4.2 Insert a record of the output of the action into the database
		try{
			pAction = this.aPersistance.updateActionState(pAction, ActionState.FINISHED);
			logger.log(Level.FINE, "Changed status of action {0} to FINISHED", actionId);
			
			//1. Updating start and end time.
			pAction = this.aPersistance.getActionById(actionId.toHexString());
			String oozieId = pAction.getSubmissionId();
			List<Date> times = OozieClientUtil.getStartAndEndTime(oozieId);
			Date startTime = times.get(0);
			Date endTime = times.get(1);
			//2. Inserting a record of the output action into the database.
			String outputPath = pAction.getAction().getOutputPath();
			Double sizeInMB = HdfsUtil.getSizeInMB(outputPath);
			
			this.aPersistance.addStartAndEndTimeAndSize(pAction, startTime, endTime, sizeInMB);
			logger.log(Level.FINER, "Action {0} startTime: {1}, endTime: {2}, sizeInMB: {3}", new Object[]{ actionId, startTime, endTime, sizeInMB});
			
			if (null != sizeInMB) {
				PersistedDataset actionDataset = this.dPersistance.getDatasetByPath(outputPath);
				if (null == actionDataset) {
					PersistedDataset newDataset = new PersistedDataset(outputPath,
							sizeInMB, DatasetState.STORED, new Date(), 1, childActionIds);
					this.dPersistance.insertDataset(newDataset);
					logger.log(Level.FINER, "Inserted new dataset {0} for action {1} with state STORED", new Object[]{outputPath, actionId});
					this.dLogDao.insertLogEntry(outputPath, DatasetState.PROCESSING, DatasetState.STORED, sizeInMB);
				}
				else {
					DatasetState state = actionDataset.getState();
					if (state.equals(DatasetState.TO_STORE)) {
						actionDataset = this.dPersistance.updateDatasetState(actionDataset, DatasetState.STORED);
						logger.log(Level.FINER, "Updated state of dataset {0} to STORED", outputPath);
						this.dLogDao.insertLogEntry(outputPath, DatasetState.TO_STORE, DatasetState.STORED, sizeInMB);
					}
					else if (DatasetState.TO_LEAF.equals(state)) {
						actionDataset = this.dPersistance.updateDatasetState(actionDataset, DatasetState.LEAF);
						logger.log(Level.FINER, "Updated state of dataset {0} to LEAF", outputPath);
						this.dLogDao.insertLogEntry(outputPath, DatasetState.TO_LEAF, DatasetState.LEAF, sizeInMB);
					}
					else if (state.equals(DatasetState.TO_DELETE)){
						actionDataset = this.dPersistance.updateDatasetState(actionDataset, DatasetState.STORED_TO_DELETE);
						logger.log(Level.FINER, "Updated state of dataset {0} to STORED_TO_DELETE", outputPath);
						this.dLogDao.insertLogEntry(outputPath, DatasetState.TO_DELETE, DatasetState.STORED_TO_DELETE, sizeInMB);
					}
					this.dPersistance.updateDatasetSizeInMB(actionDataset, sizeInMB);
					for (String childActionId : childActionIds) {
						this.dPersistance.addClaimToDataset(actionDataset, childActionId);
						logger.log(Level.FINER, "Added claim to dataset {0} from child action id {1}", new Object[]{outputPath, childActionId});
					}
					
				}
				
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.log(Level.WARNING, "Exception thrown " + e.toString());
		}
		
	}
	
	public void actionFailed(PersistedAction pAction) {
		
		ObjectId actionId = pAction.getId();
		
		logger.log(Level.FINE, "actionFailed called on action {0}", actionId);
		//1. Decrease dataset claims
		decreaseDatasetClaims(actionId.toHexString());
		
		//2. Remove the output produced by the action
		//and update the database with the output state accordingly.
		//In order to remove the output, just mark the dataset with
		//state TO_DELETE, and let the other guys handle it.
		try{
			pAction = this.aPersistance.getActionById(actionId.toHexString());
			String outputPath = pAction.getAction().getOutputPath();
			PersistedDataset dataset = this.dPersistance.getDatasetByPath(outputPath);
			//TODO: There might be a bug here. Analyze later: If the dataset is not deleted immediately here
			//then it could be that actions with claims on it might use it even when it is in an inconsistent state.
			//No, it is not possible.  Actions get only notified of this dataset if
			//the action succeeded.  No actions can possibly have a claim on this
			//dataset unless they are having it through the action, because this
			//dataset did not exist, and no action places a clain on a non-existent 
			//dataset.
			this.dPersistance.updateDatasetState(dataset, DatasetState.STORED_TO_DELETE);
			logger.log(Level.FINE, "Updated state of dataset {0} to STORED_TO_DELETE", dataset.getPath());
			
			//3. Update the action state to FAILED
			this.aPersistance.updateActionState(pAction, ActionState.FAILED);
			
			logger.log(Level.FINE, "Updated state of action {0} to FAILED", actionId);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.log(Level.WARNING, "Exception thrown " + e.toString());
		}
		
		
	}
	
	public void actionKilled(PersistedAction pAction) {
		ObjectId actionId = pAction.getId();
		logger.log(Level.FINE, "actionKilled called on action {0}", actionId);
		//1. Decrease dataset claims
		decreaseDatasetClaims(actionId.toHexString());
		
		//2. Remove the output produced by the action and
		//update the database with the output state accordingly.
		//IMPORTANT: For the sake of concurrency correctness,
		//we need to remove the output produced by the action
		//before updating the state of the action to KILLED.
		try {
			pAction = this.aPersistance.getActionById(actionId.toHexString());
			String outputPath = pAction.getAction().getOutputPath();
			PersistedDataset dataset = this.dPersistance.getDatasetByPath(outputPath);
			this.dPersistance.updateDatasetState(dataset, DatasetState.STORED_TO_DELETE);
			logger.log(Level.FINE, "Updated state of dataset {0} to STORED_TO_DELETE", dataset.getPath());
			
			//3. Update the state of the action.
			
			this.aPersistance.updateActionState(pAction, ActionState.KILLED);
			logger.log(Level.FINE, "Updated state of action {0} to KILLED", actionId);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.log(Level.WARNING, "Exception thrown " + e.toString());
		}

		
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
