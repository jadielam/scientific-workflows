package io.biblia.workflows.decision;

import java.util.HashSet;

import io.biblia.workflows.definition.Dataset;
import io.biblia.workflows.definition.Workflow;

public class StubStorageDecider implements DatasetStorageDecider {

	/**
	 * It always decides to not store any of the datasets of the
	 * current workflow, as well as not do delete any of the 
	 * datasets currently on file system.
	 */
	@Override
	public DecisionResponse decide(Workflow w) {
		return new DecisionResponse(new HashSet<Dataset>(), new HashSet<Dataset>());
	}
	
}
