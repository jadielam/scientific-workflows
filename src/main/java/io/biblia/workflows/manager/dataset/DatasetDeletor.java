package io.biblia.workflows.manager.dataset;

import java.io.IOException;

import com.google.common.base.Preconditions;
import io.biblia.workflows.hdfs.HdfsUtil;
import io.biblia.workflows.manager.decision.DatasetLogDao;

public class DatasetDeletor implements Runnable {

	private PersistedDataset dataset;
	private final DatasetPersistance persistance;
	private final DatasetLogDao datasetLogDao;
	
	public DatasetDeletor(PersistedDataset dataset, DatasetPersistance persistance,
			DatasetLogDao datasetLogDao) {
		Preconditions.checkNotNull(dataset);
		Preconditions.checkNotNull(persistance);
		Preconditions.checkNotNull(datasetLogDao);
		this.dataset = dataset;
		this.persistance = persistance;
		this.datasetLogDao = datasetLogDao;
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
	 * Deletes the data from the path where it is in. If it cannot delete it
	 * it marks it with state TO_DELETE again.
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
		try {
			HdfsUtil.deletePath(this.dataset.getPath());
		}
		catch(IOException ex) {
			try{
				this.dataset = this.persistance.updateDatasetState(this.dataset, DatasetState.STORED_TO_DELETE);
			}
			catch(Exception ex1) {
				//Ignore it. I am going to return. THe system will clean up itself.
			}
			return;
		}
		
		try {
			this.dataset = this.persistance.updateDatasetState(this.dataset, DatasetState.DELETED);
			this.datasetLogDao.insertLogEntry(this.dataset.getPath(), DatasetState.DELETING, DatasetState.DELETED, this.dataset.getSizeInMB());
		}
		catch (Exception e) {
			return;
		}
	}
	
}
