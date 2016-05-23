package io.biblia.workflows.manager.dataset;

import io.biblia.workflows.definition.Dataset;
import org.bson.types.ObjectId;

import com.google.common.base.Preconditions;

import java.util.Date;

public class PersistedDataset extends Dataset {

	/**
	 * The current state of the dataset
	 */
	private final DatasetState state;
	
	/**
	 * Last time the entry was updated in mongodb
	 */
	private final Date lastUpdatedDate;
	
	/**
	 * version field used to compare if I have the
	 * current version of the object.
	 * It is increased each time that the dataset
	 * is updated
	 */
	private final int version;
	
	/**
	 * It registers the number of claims that
	 * a dataset has.  A dataset has claims whenever
	 * a task that is going to run depends on it.
	 */
	private final int claims;
	
	public PersistedDataset(String path, Double sizeInMB, DatasetState state,
			Date lastUpdatedDate, int version, int claims) {
		super(path, sizeInMB);
		Preconditions.checkNotNull(state);
		Preconditions.checkNotNull(lastUpdatedDate);
		this.state = state;
		this.lastUpdatedDate = lastUpdatedDate;
		this.version = version;
		this.claims = claims;
	}
	
	public int getClaims() {
		return this.claims;
	}
	
	public DatasetState getState() {
		return this.state;
	}
	
	public Date getLastUpdatedDate() {
		return this.lastUpdatedDate;
	}
	
	public int getVersion() {
		return this.version;
	}
	
}
