package io.biblia.workflows.definition;

import org.bson.Document;

/**
 * Contains information about the dataset
 * For now it has a path, I have to add 
 * @author dearj019
 *
 */
public class Dataset implements DatasetAttributesConstants {

	private final String path;
	
	private final Integer sizeInMB;
	
	public Dataset(String path, Integer sizeInMB) {
		this.path = path;
		this.sizeInMB = sizeInMB;
	}

	public String getPath() {
		return path;
	}
	
	public Integer getSizeInMB() {
		return this.sizeInMB;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((path == null) ? 0 : path.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Dataset other = (Dataset) obj;
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		return true;
	}
	
	public Document toBson() {
		Document toReturn = new Document();
		toReturn.append(DATASET_PATH, this.getPath());
		toReturn.append(DATASET_SIZE_IN_MB, this.getSizeInMB());
		return toReturn;
		
	}
}
