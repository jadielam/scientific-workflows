package io.biblia.workflows.manager.action;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Preconditions;

public class ActionManager {

	private static ActionManager instance = null;
	
	private final BlockingQueue<ProcessingAction> actionsQueue;
	
	private final ExecutorService actionSubmittersExecutor;
	
	private static final int NUMBER_OF_ACTION_SUBMITTERS = 5;
	/**
	 * Sets up a database scrapper, a concurrent queue
	 * and a pool of action submitters.
	 * 
	 * The database scrapper fills the pool with actions
	 * that are available to be submitted.
	 * 
	 * The action submitter empties the queue of actions and
	 * submits the actions to Apache Oozie or Hadoop.
	 * 
	 * The concurrent queue receives from the scrapper and
	 * gives to the submitter.
	 */
	private ActionManager(ActionPersistance actionPersistance) {
		
		Preconditions.checkNotNull(actionPersistance);
		//1. Create the concurrent queue.
		this.actionsQueue = new LinkedBlockingQueue<ProcessingAction>();
	
		//2. Start the Database scraper.
		ActionScraper.start(this.actionsQueue, actionPersistance);
		
		//3. Create the pool of action submitters
		this.actionSubmittersExecutor = 
				Executors.newFixedThreadPool(NUMBER_OF_ACTION_SUBMITTERS);
		
		for (int i = 0; i < NUMBER_OF_ACTION_SUBMITTERS; ++i) {
			this.actionSubmittersExecutor.execute(new ActionSubmitter(this.actionsQueue));
		}
		
		//I want this to run forever. How can I make this to 
		//work properly?
		//executor.shutdown();
		
	}
	
	public static ActionManager getInstance(ActionPersistance persistance) {
		if (null == instance) {
			instance = new ActionManager(persistance);
		}
		
		return instance;
	}
	
	private void finishActionSubmitters() {
		
	}
	
	private void finishActionScraper() {
		ActionScraper.stop();
	}
}
