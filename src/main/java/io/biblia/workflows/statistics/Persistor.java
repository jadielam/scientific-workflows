package io.biblia.workflows.statistics;

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

	private Persistor() {
		
	}
	
	public void addWorkflow(Workflow workflow) {
		
	}
	
	public void addAction(Action action) {
		
	}
	
	public void addExecutionTimeToAction(String actionId, long milliseconds) {
		
	}
	
	public void addStorageSpaceToDataset(String dataset, long megabytes) {
		
	}
	
	public void addSavedDataset(Dataset dataset) {
		
	}
	
	public void removeFromSavedDataset(Dataset dataset) {
		
	}
}
