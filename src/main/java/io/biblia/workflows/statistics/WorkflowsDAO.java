package io.biblia.workflows.statistics;

import org.bson.types.ObjectId;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.Dataset;
import io.biblia.workflows.definition.Workflow;

public interface WorkflowsDAO {

	String addWorkflow(Workflow workflow);

	String addAction(Action action);

	String addSavedDataset(Dataset dataset);

	void addExecutionTimeToAction(String actionId, long milliseconds);

	void addStorageSpaceToDataset(String datasetPath, long megabytes);

}