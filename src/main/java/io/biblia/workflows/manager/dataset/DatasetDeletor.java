package io.biblia.workflows.manager.dataset;

import com.google.common.base.Preconditions;

import io.biblia.workflows.definition.Dataset;

public class DatasetDeletor implements Runnable {

	private PersistedDataset dataset;
	private final DatasetPersistance persistance;
	private static final String DELETE_ACTION_ROOT_FOLDER = "TODO";
	
	public DatasetDeletor(PersistedDataset dataset, DatasetPersistance persistance) {
		Preconditions.checkNotNull(dataset);
		Preconditions.checkNotNull(persistance);
		this.dataset = dataset;
		this.persistance = persistance;
	}

	@Override
	public void run() {
		try {
			this.deleteDataset();
		}
		catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	/**
	 * Creates oozie action that will take care of deleting the data.
	 * @param dataset
	 */
	private void deleteDataset() {
		
		//1. Update database with DELETING state
		try {
			this.dataset = this.persistance.updateDatasetState(dataset, DatasetState.DELETING);
		}
		catch (OutdatedDatasetException ex) {
			return;
		}
		catch (Exception ex) {
			return;
		}
		
		//1.2.1 Delete the datasets on that folder and if it succeeds, update the
		//state to deleted, otherwise, if there is an error
		
		//1.2.1.1 If there is an error submitting the action the dataset, 
		//update database with state TO_DELETE
		
	}
	
	private String generateActionFolder() {
		String id = this.dataset.getId().toString();
		return DELETE_ACTION_ROOT_FOLDER + "/" + id;
	}
	
}
