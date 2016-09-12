package io.biblia.workflows.manager.decision;

import java.util.List;
import java.util.LinkedList;

import com.google.common.base.Preconditions;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import org.apache.commons.collections4.queue.CircularFifoQueue;

import io.biblia.workflows.manager.DatabaseConstants;
import io.biblia.workflows.manager.action.ActionState;
import io.biblia.workflows.manager.action.PersistedAction;

import static com.mongodb.client.model.Filters.*;

public class ActionRollingWindow implements DatabaseConstants {

	private final MongoClient mongo;
	private final MongoDatabase workflows;
	private final MongoCollection<Document> actions;
	
	private static ActionRollingWindow instance = null;
	
	private Long marker = null;
	private static int QUEUE_LIMIT = 400;
	private CircularFifoQueue<PersistedAction> cache = new CircularFifoQueue<PersistedAction>(QUEUE_LIMIT);
	
	private ActionRollingWindow(MongoClient mongo) {
		this.mongo = mongo;
		this.workflows = this.mongo.getDatabase(WORKFLOWS_DATABASE);
		this.actions = this.workflows.getCollection(ACTIONS_COLLECTION);	
	}

	/**
	 * Returns the last n actions that have either been FINISHED,
	 * KILLED, or FAILED. If the total number of actions that have been
	 * FINISHED, KILLED or FAILED is less than n, then it returns them all.
	 * @param n
	 * @return
	 */
	public List<PersistedAction> getLastActions(int n) {
		Preconditions.checkArgument(n > 0);
		Preconditions.checkArgument(n <= QUEUE_LIMIT);
		
		FindIterable<Document> documents;
		
		if (null == this.marker) {
			documents = this.actions.find(
				or(
					eq("state", ActionState.FINISHED),
					eq("state", ActionState.FAILED),
					eq("state", ActionState.KILLED),
					eq("state", ActionState.COMPUTED)
				)
			).skip((int)Math.max(0, (this.actions.count() - QUEUE_LIMIT)));
			//TODO: I should file a bug report here, since it is 
			//understandable that I might need a long in some large
			//collections.
		}
		else {
			documents = this.actions.find(
				and(
					or(
						eq("state", ActionState.FINISHED),
						eq("state", ActionState.FAILED),
						eq("state", ActionState.KILLED),
						eq("state", ActionState.COMPUTED)
					),
					gt("marker", this.marker)
				)
			).skip((int) Math.max(0, this.actions.count() - QUEUE_LIMIT));
		}
		
		MongoCursor<Document> iterator = documents.iterator();
		try {
			while (iterator.hasNext()) {
				Document next = iterator.next();
				Long newMarker = next.getLong("marker");
				this.marker = newMarker;
				try{
					PersistedAction action = PersistedAction.parseAction(next);
					this.cache.add(action);
				}
				catch(Exception ex) {
					continue;
				}
			}
		}
		finally {
			iterator.close();
		}
		
		//Returns the last n documents from the cache.
		List<PersistedAction> toReturn = new LinkedList<PersistedAction>();
		for (int i = this.cache.size() - 1; i >= Math.max(0, this.cache.size() - 1 - n); --i) {
			toReturn.add(this.cache.get(i));
		}
		
		return toReturn;
	}
		
	public static ActionRollingWindow getInstance(MongoClient mongo) {
		if (null == instance) {
			instance = new ActionRollingWindow(mongo);
		}
		return instance;
	}
	
}
