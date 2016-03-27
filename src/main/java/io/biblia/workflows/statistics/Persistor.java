package io.biblia.workflows.statistics;

import org.bson.types.ObjectId;

import io.biblia.workflows.EnvironmentVariables;
import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.Dataset;
import io.biblia.workflows.definition.Workflow;

/**
 * Takes are of persisting the data
 * into a databse.
 * @author jadiel
 *
 */
public class Persistor {

	private static Persistor instance; 
	private final WorkflowsDAO dao;
	
	private Persistor() {
		this.dao = WorkflowsDAOBuilder.getInstance(WorkflowsDAOBuilder.MONGODB_TYPE);
	}
	
	public void addWorkflow(Workflow workflow) {
		this.dao.addWorkflow(workflow);
	}
	
	public void addAction(Action action) {
		this.dao.addAction(action);
	}
	
	public void addExecutionTimeToAction(String actionId, long milliseconds) {
		this.dao.addExecutionTimeToAction(actionId, milliseconds);
	}
	
	public void addStorageSpaceToDataset(String dataset, long megabytes) {
		this.dao.addStorageSpaceToDataset(dataset, megabytes);
	}
	
	public void addSavedDataset(Dataset dataset) {
		
	}
	
	public void removeFromSavedDataset(Dataset dataset) {
		
	}
	
	public static Persistor getInstance() {
		if (null == instance) {
			instance = new Persistor();
		}
		return instance;
	}
}
