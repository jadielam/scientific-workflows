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
	 * Means that the dataset, when stored will be marked as a leaf
	 */
	TO_LEAF,
	
	/**
	 * Dataset is stored in file system and can be used and reused.
	 * It can be deleted at any time if decision algorithm determines
	 * that it is no longer useful.
	 */
	STORED, 
	
	/**
	 * State of dataset that will stay stored forever.
	 */
	LEAF,
	
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
