package io.biblia.workflows.manager.decision;

import java.util.List;

import io.biblia.workflows.manager.dataset.PersistedDataset;

public interface DecisionAlgorithm {

	/**
	 * Given a simplified workflow and the space to free, it determines 
	 * which of the datasets from the workflow history will be removed.
	 * @param workflow
	 * @param spaceToFree
	 * @return
	 */
	public List<String> toDelete(SimplifiedWorkflowHistory workflow, List<PersistedDataset> storedDatasets,
			Long spaceToFree);
}
