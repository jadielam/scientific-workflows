package io.biblia.workflows.statistics;

import org.bson.types.ObjectId;

import io.biblia.workflows.EnvironmentVariables;
import io.biblia.workflows.definition.ManagedAction;
import io.biblia.workflows.definition.Dataset;
import io.biblia.workflows.definition.Workflow;

/**
 * Takes are of persisting the data
 * into a databse.
 * @author jadiel
 *
 */
public class Registrar {

	private static Registrar instance; 
	private final WorkflowsDAO dao;
	
	private Registrar() {
		this.dao = WorkflowsDAOBuilder.getInstance(WorkflowsDAOBuilder.MONGODB_TYPE);
	}
	
	public void addWorkflow(Workflow workflow) {
		this.dao.addWorkflow(workflow);
	}
	
	public void addAction(ManagedAction action) {
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
	
	public static Registrar getInstance() {
		if (null == instance) {
			instance = new Registrar();
		}
		return instance;
	}
}
