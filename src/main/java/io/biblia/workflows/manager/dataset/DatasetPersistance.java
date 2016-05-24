package io.biblia.workflows.manager.dataset;

import java.util.List;
import io.biblia.workflows.definition.Dataset;
import io.biblia.workflows.definition.parser.DatasetParseException;

public interface DatasetPersistance {

	/**
	 * Returns the datasets with state TO_DELETE and claims put on them
	 * equal to 0, and all the datasets with state DELETING and lastUpdated
	 * date more than a given constant. It returns only the number given 
	 * by the parameter number
	 * @param number
	 * @return
	 */
	public List<PersistedDataset> getDatasetsToDelete(int number);
	
	/**
	 * Updates the state of the dataset and returns the new updated dataset.
	 * @param dataset
	 * @param newState
	 * @return
	 * @throws OutdatedDatasetException
	 */
	PersistedDataset updateDatasetState(PersistedDataset dataset, 
			DatasetState newState) throws OutdatedDatasetException, DatasetParseException;
	
	/**
	 * Inserts the dataset into MongoDB. If the dataset already exists, it
	 * replaces by a new one with version and lastUpdatedDate initialized new.
	 * @param dataset
	 * @return the id of the dataset inserted as a String
	 */
	String insertDataset(PersistedDataset dataset);
	
	/**
	 * Adds the given action id to the list of actions that depend on this
	 * dataset. When a dataset is initially created, it is given an initial 
	 * list of claims. New claims are added as new actions that did not
	 * exist yet are created that depend on this dataset.
	 * @param datasetPath
	 * @param actionId
	 * @return the updated dataset
	 */
	PersistedDataset addClaimToDataset(String datasetPath, String actionId) throws DatasetParseException;
	
	/**
	 * Removes the action id from the list of actions that depend on a dataset.
	 * It is usually called whenever an action has finished executing.
	 * @param datasetPath
	 * @param actionId
	 * @return
	 */
	PersistedDataset removeClaimFromDataset(String datasetPath, String actionId) throws DatasetParseException;
	
	/**
	 * Removes all the claims that this action has over datasets.
	 * @param actionId
	 */
	void removeClaimFromDatasets(String actionId);
}

class OutdatedDatasetException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2531448080808892124L;
	
}
