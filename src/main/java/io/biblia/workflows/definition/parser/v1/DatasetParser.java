package io.biblia.workflows.definition.parser.v1;

import org.bson.Document;

import io.biblia.workflows.definition.Dataset;
import io.biblia.workflows.definition.DatasetAttributesConstants;
import io.biblia.workflows.definition.parser.DatasetParseException;

public class DatasetParser extends io.biblia.workflows.definition.parser.DatasetParser 
implements DatasetAttributesConstants {

	@Override
	public Dataset parseDataset(Document object) throws DatasetParseException {
		
		String path = object.getString(DATASET_PATH);
		if (null == path) {
			throw new DatasetParseException("The dataset does not have a <path> attribute");
		}
		Integer sizeInMB = object.getInteger(DATASET_SIZE_IN_MB);
		if (null == sizeInMB) {
			throw new DatasetParseException("The dataset does not have a <sizeInMB> attribute");
		}
		return new Dataset(path, sizeInMB);
	}

}
