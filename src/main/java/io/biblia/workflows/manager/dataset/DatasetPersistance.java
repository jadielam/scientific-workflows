package io.biblia.workflows.manager.dataset;

import java.util.List;

public interface DatasetPersistance {

	public List<PersistedDataset> getDatasetsToDelete(int number);
	
	PersistedDataset updateDatasetState(PersistedDataset dataset, 
			DatasetState newState) throws OutdatedDatasetException;
}

class OutdatedDatasetException extends Exception {
	
}
