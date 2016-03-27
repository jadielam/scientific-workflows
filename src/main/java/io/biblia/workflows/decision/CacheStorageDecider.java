package io.biblia.workflows.decision;

import io.biblia.workflows.definition.Workflow;

public class CacheStorageDecider implements DatasetStorageDecider {

	/**
	 * Uses the history of the submissions to the system to determine
	 * which workflows to keep and which to remove.
	 */
	@Override
	public DecisionResponse decide(Workflow w) {
		// TODO Auto-generated method stub
		return null;
	}

}
