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
	 * Returns the id of the dataset inserted.
	 * @param dataset
	 * @return
	 */
	String insertDataset(Dataset dataset);
}

class OutdatedDatasetException extends Exception {
	
}
