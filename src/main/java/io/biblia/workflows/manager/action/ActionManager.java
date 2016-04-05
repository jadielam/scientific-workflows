package io.biblia.workflows.manager.action;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Preconditions;

public class ActionManager {

	private static ActionManager instance = null;
	
	private static Thread t;
	
	private static final BlockingQueue<ProcessingAction> actionsQueue;
	
	private static final ExecutorService actionSubmittersExecutor;
	
	private static final int NUMBER_OF_ACTION_SUBMITTERS = 5;
	
	static {
		//1. Create the concurrent queue.
		actionsQueue = new LinkedBlockingQueue<ProcessingAction>();
		
		//2. Create the executors.
		actionSubmittersExecutor = 
						Executors.newFixedThreadPool(NUMBER_OF_ACTION_SUBMITTERS);
	}
	
	private class ActionManagerRunner implements Runnable {

		@Override
		public void run() {
			while(!Thread.currentThread().isInterrupted()) {
				
				try {
					ProcessingAction action = actionsQueue.take();
					actionSubmittersExecutor.execute(new ActionSubmitter(action));
					Thread.sleep(100);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
	
		}
		
	}
	
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
		
		//2. Start the Database scraper.
		ActionScraper.start(actionsQueue, actionPersistance);
		
		t = new Thread(new ActionManagerRunner(), "ActionManager thread");
		
		t.start();
		
	}
	
	public static void start(ActionPersistance persistance) {
		if (null == instance) {
			instance = new ActionManager(persistance);
		}
	}
	
	public static void stop() {
		finishActionScraper();
		finishActionSubmitters();
		t.interrupt();
	}
	
	private static void finishActionSubmitters() {
		actionSubmittersExecutor.shutdown();
	}
	
	private static void finishActionScraper() {
		ActionScraper.stop();
	}
}
