package io.biblia.workflows.manager.dataset;

import java.util.List;
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
	 * Returns the path of all the paths that have been stored
	 * @return
	 */
	public List<String> getAllStoredDatasetPaths();
	
	/**
	 * Returns all the datasets that have state STORED
	 * @return
	 */
	public List<PersistedDataset> getAllStoredDatasets();

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
	 * Updates the size of the dataset and returns the new updated dataset.
	 * @param dataset
	 * @param sizeInMB
	 * @return
	 * @throws OutdatedDatasetException
	 * @throws DatasetParseException
	 */
	PersistedDataset updateDatasetSizeInMB(PersistedDataset dataset,
			Double sizeInMB) throws OutdatedDatasetException, DatasetParseException;
	
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
	PersistedDataset addClaimToDataset(PersistedDataset dataset, String actionId) throws DatasetParseException, OutdatedDatasetException;
	
	/**
	 * Removes the action id from the list of actions that depend on a dataset.
	 * It is usually called whenever an action has finished executing.
	 * @param datasetPath
	 * @param actionId
	 * @return
	 */
	PersistedDataset removeClaimFromDataset(PersistedDataset dataset, String actionId) throws DatasetParseException, OutdatedDatasetException;
	
	/**
	 * Removes all the claims that this action has over datasets.
	 * @param actionId
	 */
	void removeClaimFromDatasets(String actionId);
	
	/**
	 * Retrieves the dataset from the database by the path that is has assigned to it.
	 * The path of a dataset is its primary key.
	 * @param outputPath
	 * @return
	 */
	PersistedDataset getDatasetByPath(String outputPath) throws DatasetParseException;
}
