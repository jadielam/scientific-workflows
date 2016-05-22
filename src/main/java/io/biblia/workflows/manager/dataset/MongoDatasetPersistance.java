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
import com.mongodb.client.model.UpdateOptions;
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
	
	public MongoDatasetPersistance(MongoClient mongo) {
		this.mongo = mongo;
		this.workflows = this.mongo.getDatabase(WORKFLOWS_DATABASE);
		this.datasets = this.workflows.getCollection(DATASETS_COLLECTION);
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
						eq("state", DatasetState.DELETING.name()),
						eq("state", DatasetState.PROCESSING.name())
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
		final Document filter = new Document().append("path", dataset.getPath())
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
		
		Date date = (Date) document.getDate("lastUpdatedDate");
		String stateString = document.getString("state");
		DatasetState state = DatasetState.valueOf(stateString);
		String path =  document.getString("path");
		Integer sizeInMB = document.getInteger("sizeInMB");
		int version = document.getInteger("version", 0);
		int claims = document.getInteger("claims", 0);
		
		return new PersistedDataset(path, sizeInMB, state, date, version, claims);
	}

	/**
	 * Inserts a new dataset to the collection with the status STORED
	 * on it.  If that path already exists, it repalces it with the new
	 * data.
	 * @return the ObjectId used to store it in MongoDB as a string.
	 * 
	 */
	@Override
	public String insertDataset(Dataset dataset) {
		
		final Document filter = new Document().append("path", dataset.getPath());
		final Document replace = new Document().append("version", 1)
				.append("lastUpdatedDate", new Date())
				.append("version", 1)
				.append("state", DatasetState.STORED)
				.append("sizeInMB", dataset.getSizeInMB());
		UpdateOptions options = new UpdateOptions();
		options.upsert(true);
		
		this.datasets.replaceOne(filter,  replace, options);
		return dataset.getPath();
	}

}
