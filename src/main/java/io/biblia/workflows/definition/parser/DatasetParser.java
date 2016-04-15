package io.biblia.workflows.definition.parser;

import io.biblia.workflows.definition.Dataset;
import org.bson.Document;

public abstract class DatasetParser {

	public abstract Dataset parseDataset(Document object) throws DatasetParseException;
}
