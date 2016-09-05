package io.biblia.workflows.manager.decision;

import io.biblia.workflows.manager.dataset.DatasetState;

/**
 * a) Writes logs of dataset state changes to the database.
 * b) Reads logs of datasets from database to memory maintaining a 
 *    in order to determine, based on the logs, how much space is
 *    being used by the datasets in the file system.
 * @author dearj019
 *
 */
public interface DatasetLogDao {

	/**
	 * Inserts a new entry into the log with the parameters passed
	 * @param datasetPath The path of the dataset in the file system
	 * @param previousState The previous state of the dataset
	 * @param newState The current state of the dataset
	 * @param datasetSize The current size of the dataset
	 * @return The id entry of the log.
	 */
	public String insertLogEntry(String datasetPath, DatasetState previousState,
			DatasetState newState, Double datasetSize);
	
	/**
	 * Queries the log database and calculates how much space has been 
	 * used by datasets in the file system. 
	 * @return the space used in megabytes.
	 */
	public long currentlyUsedSpace();
}
