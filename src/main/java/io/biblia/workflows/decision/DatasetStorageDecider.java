package io.biblia.workflows.decision;

import io.biblia.workflows.definition.Workflow;

/**
 * INterface that is implemented by all the algorithms that 
 * decide what datasets from a given workflow will be kept,
 * as well as what datasets currently on the file system
 * will be deleted.
 * 
 * @author jadiel
 *
 */
public interface DatasetStorageDecider  {

	public DecisionResponse decide(Workflow w);
	
}
