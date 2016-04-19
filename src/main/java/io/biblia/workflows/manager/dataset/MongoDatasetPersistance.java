package io.biblia.workflows.manager.dataset;

import java.util.Calendar;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;

import io.biblia.workflows.definition.Dataset;
import io.biblia.workflows.definition.parser.DatasetParseException;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.FindOneAndUpdateOptions;

import io.biblia.workflows.definition.parser.v1.DatasetParser;

import org.bson.Document;
import org.bson.types.ObjectId;

import static com.mongodb.client.model.Filters.*;

public class MongoDatasetPersistance implements DatasetPersistance {

	public static final String DATASETS_COLLECTION = "datasets_cl";
	public static final String WORKFLOWS_DATABASE = "workflows_db";
	
	public static final int OUTDATED_SECONDS = 400;
	private final MongoClient mongo;
	private final MongoDatabase workflows;
	private final MongoCollection<Document> datasets;
	private final io.biblia.workflows.definition.parser.DatasetParser parser;
	
	public MongoDatasetPersistance(MongoClient mongo) {
		this.mongo = mongo;
		this.workflows = this.mongo.getDatabase(WORKFLOWS_DATABASE);
		this.datasets = this.workflows.getCollection(DATASETS_COLLECTION);
		this.parser = new DatasetParser();
	}
	
	@Override
	public List<PersistedDataset> getDatasetsToDelete(int n) {
		List<PersistedDataset> toReturn = new ArrayList<>();
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.SECOND, -1 * OUTDATED_SECONDS);
		Date minus = calendar.getTime();
		
		final FindIterable<Document> documents = this.datasets.find(or(
				and(
					eq("state", DatasetState.TO_DELETE.name()),
					eq("claims", 0)
				),
				and(
					or(
						eq("state", DatasetState.PROCESSING.name()),
						eq("state", DatasetState.DELETING.name())
					),
					gte("lastUpdatedDate", minus)
				)
			)
		);
		
		if (n > 0) {
			documents.limit(n);
		}
		
		MongoCursor<Document> iterator = documents.iterator();
		try {
			while (iterator.hasNext()) {
				Document next = iterator.next();
				try {
					PersistedDataset dataset = parseDataset(next);
					toReturn.add(dataset);
				}
				catch(Exception e) {
					continue;
				}
			}
		}
		finally {
			iterator.close();
		}
		return toReturn;
	}

	@Override
	public PersistedDataset updateDatasetState(PersistedDataset dataset, DatasetState newState)
			throws OutdatedDatasetException, DatasetParseException {
		final Document filter = new Document().append("_id", dataset.getId())
				.append("version", dataset.getVersion());
		final Document update = new Document().append("$set", new Document("state", newState.name()))
				.append("$currentDate", new Document("lastUpdatedDate", true))
				.append("$inc",new Document("version", 1));
		
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
		options.returnDocument(ReturnDocument.AFTER);
		Document newDocument = this.datasets.findOneAndUpdate(filter, update, options);
		if (null == newDocument) {
			throw new OutdatedDatasetException();
		}
		else {
			return parseDataset(newDocument);
		}
	}
	
	private PersistedDataset parseDataset(Document document) throws DatasetParseException {
		
		ObjectId id = document.getObjectId("_id");
		Date date = (Date) document.getDate("lastUpdatedDate");
		String stateString = document.getString("state");
		DatasetState state = DatasetState.valueOf(stateString);
		Document datasetDoc = (Document) document.get("dataset");
		Dataset dataset = this.parser.parseDataset(datasetDoc);
		int version = document.getInteger("version", 0);
		
		return new PersistedDataset(dataset, id, state, date, version);
	}

	@Override
	public String insertDataset(Dataset dataset) {
		//TODO;
		return null;
	}

}
