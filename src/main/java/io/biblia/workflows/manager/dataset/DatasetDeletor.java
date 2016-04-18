package io.biblia.workflows.manager.dataset;

import com.google.common.base.Preconditions;

import io.biblia.workflows.definition.Dataset;

public class DatasetDeletor implements Runnable {

	private final PersistedDataset dataset;
	private final DatasetPersistance persistance;
	
	public DatasetDeletor(PersistedDataset dataset, DatasetPersistance persistance) {
		Preconditions.checkNotNull(dataset);
		Preconditions.checkNotNull(persistance);
		this.dataset = dataset;
		this.persistance = persistance;
	}

	@Override
	public void run() {
		try {
			this.deleteDataset(this.dataset);
		}
		catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	private void deleteDataset(PersistedDataset dataset) {
		
		//1. Update database with DELETING state
		try {
			dataset = this.persistance.updateDatasetState(dataset, DatasetState.DELETING);
		}
		catch (OutdatedDatasetException ex) {
			return;
		}
		catch (Exception ex) {
			return;
		}
		//1.2.1 IF database accepts update by comparing versions
		//create Oozie action that deletes dataset or folder
		
		//1.2.2 Persist oozie action to the database with a callback
		//endpoint that specializes in handling dataset deletion actions.
		//Add deletion action id to dataset so that the callback is able
		//to recover the deletion action back from dataset id.
		//The callback endpoint will both update the state of the
		//dataset and of the action created to delete the dataset.
		
		//1.2.1.1 If there is an error submitting the action the dataset, 
		//update database with state TO_DELETE
		
		//1.2.2 Callback endpoint: by dataset it
		
	}
	
}
