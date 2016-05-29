package io.biblia.workflows.manager.dataset;

public enum DatasetState {

	/**
	 * The dataset does not exist, but once it does, it will be 
	 * transitioned to the state STORED_TO_DELETE
	 */
	TO_DELETE,
	/**
	 * The dataset does not exist, but once it does, it will
	 * be transitioned to the state STORED
	 */
	TO_STORE,
	/**
	 * Used to signify that a dataset has been stored as a long term
	 * dataset.
	 */
	STORED, 
	/**
	 * Dataset is stored temporarily until all the claims have been fulfilled.
	 * Once the dataset does not have any more claims, it is deleted.
	 */
	STORED_TO_DELETE, 
	/**
	 * The dataset is being processed with the purpose of deleting it.
	 */
	PROCESSING, 
	/**
	 * The dataset is being deleted.
	 */
	DELETING, 
	/**
	 * The dataset has been deleted.
	 */
	DELETED;
}
