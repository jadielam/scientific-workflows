package io.biblia.workflows.definition.parser.v1;

import org.bson.Document;

import io.biblia.workflows.definition.Dataset;
import io.biblia.workflows.definition.parser.DatasetParseException;

public class DatasetParser extends io.biblia.workflows.definition.parser.DatasetParser{

	@Override
	public Dataset parseDataset(Document object) throws DatasetParseException {
		
		String path = object.getString("path");
		if (null == path) {
			throw new DatasetParseException("The dataset does not have a <path> attribute");
		}
		return new Dataset(path);
	}

}
