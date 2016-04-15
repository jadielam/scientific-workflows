package io.biblia.workflows.manager.dataset;

import io.biblia.workflows.definition.Dataset;
import org.bson.types.ObjectId;

import com.google.common.base.Preconditions;

import java.util.Date;

public class PersistedDataset {

	private final Dataset dataset;
	private final ObjectId _id;
	private final DatasetState state;
	private final Date lastUpdatedDate;
	private final int version;
	
	public PersistedDataset(Dataset dataset, ObjectId _id,
			DatasetState state, Date lastUpdatedDate,
			int version) {
		Preconditions.checkNotNull(dataset);
		Preconditions.checkNotNull(_id);
		Preconditions.checkNotNull(state);
		Preconditions.checkNotNull(lastUpdatedDate);
		this.dataset = dataset;
		this._id = _id;
		this.state = state;
		this.lastUpdatedDate = lastUpdatedDate;
		this.version = version;
	}
	
	public Dataset getDataset() {
		return this.dataset;
	}
	
	public ObjectId getId() {
		return this._id;
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
