package io.biblia.workflows.manager.dataset;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.google.common.base.Preconditions;
import io.biblia.workflows.hdfs.HdfsUtil;
import io.biblia.workflows.manager.decision.DatasetLogDao;

public class DatasetDeletor implements Runnable {

	private PersistedDataset dataset;
	private final DatasetPersistance persistance;
	private final DatasetLogDao datasetLogDao;
	private static final Logger logger = Logger.getLogger(DatasetDeletor.class.getName());
	
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
			logger.log(Level.FINE, "Updated state of dataset {0} to DELETING", dataset.getPath());
		}
		catch (OutdatedDatasetException ex) {
			logger.log(Level.WARNING, "Could not update state of dataset {0} to DELETING, because OutdatedDatasetException was thrown", dataset.getPath());
			return;
		}
		catch (Exception ex) {
			logger.log(Level.SEVERE, "Could not update state of dataset " + dataset.getPath() + " because unknown exception was thrown", ex);
			return;
		}
		
		//1.2.1 Delete the datasets on that folder and if it succeeds, update the
		//state to deleted, otherwise, if there is an error
		try {
			HdfsUtil.deletePath(this.dataset.getPath());
			logger.log(Level.FINE, "Deleted dataset {0}", dataset.getPath());
		}
		catch(IOException ex) {
			try{
				this.dataset = this.persistance.updateDatasetState(this.dataset, DatasetState.STORED_TO_DELETE);
				logger.log(Level.FINE, "IOException thrown when attempting to delete dataset {0}. Changed back its state to STORED_TO_DELETE", dataset.getPath());
			}
			catch(Exception ex1) {
				logger.log(Level.WARNING, "Unknown exception thrown when attempting to delete dataset "+ dataset.getPath(), ex);
				//Ignore it. I am going to return. THe system will clean up itself.
			}
			return;
		}
		
		try {
			this.dataset = this.persistance.updateDatasetState(this.dataset, DatasetState.DELETED);
			logger.log(Level.FINE, "Updated state of dataset {0} to DELETED", dataset.getPath());
			this.datasetLogDao.insertLogEntry(this.dataset.getPath(), DatasetState.DELETING, DatasetState.DELETED, this.dataset.getSizeInMB());
			logger.log(Level.FINE, "Added log entry to dataset log showing change of state of dataset {0} from state DELETING to state DELETED", dataset.getPath());
		}
		catch (Exception e) {
			logger.log(Level.WARNING, "Unknown exception thrown when logging data about dataset " + dataset.getPath(), e);
			return;
		}
	}
	
}
