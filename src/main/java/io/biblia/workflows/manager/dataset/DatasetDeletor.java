package io.biblia.workflows.manager.dataset;

import com.google.common.base.Preconditions;

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
		//TODO: Implement here.
	}
	
}
