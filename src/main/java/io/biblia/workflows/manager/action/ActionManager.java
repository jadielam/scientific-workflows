package io.biblia.workflows.manager.action;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Preconditions;

import io.biblia.workflows.definition.Action;

/**
 * The action manager's purpose is to submit actions to Hadoop, in this case
 * using Apache Oozie as intermediary.  It uses the database to synchronize
 * with other processes that might also be submitting actions to Hadoop.
 * It works as follows:
 * - It has an actions queue with the actions that need to be submitted.
 * - It has an actions scraper that queries the database for actions to 
 * be submitted. THe scraper fills the actions queue with actions
 * - The ActionManager takes new actions from the queue and hands them
 * to ActionSubmitter threads that will submit the actions to Oozie and 
 * updat the database accordingly.
 * 
 * In order to support multiple servers doing the same process at the same time,
 * and to avoid the need of using a synchronization server such as Apache Zookeeper,
 * I have implemented synchronization in the following way:
 * 
 * 1. Actions can be in mutiple state. To see the possible action states @see ActionState.
 * There are listed here too: READY, PROCESSING, SUBMITTED, RUNNING, FINISHED,
	FAILED, KILLED.
 */
public class ActionManager {

	private static ActionManager instance = null;
	
	private static Thread t;
	
	private static final BlockingQueue<Action> actionsQueue;
	
	private static final ExecutorService actionSubmittersExecutor;
	
	private static final int NUMBER_OF_ACTION_SUBMITTERS = 5;
	
	static {
		//1. Create the concurrent queue.
		actionsQueue = new LinkedBlockingQueue<>();
		
		//2. Create the executors.
		actionSubmittersExecutor = 
						Executors.newFixedThreadPool(NUMBER_OF_ACTION_SUBMITTERS);
	}
	
	private class ActionManagerRunner implements Runnable {

		@Override
		public void run() {
			while(!Thread.currentThread().isInterrupted()) {
				
				try {
					Action action = actionsQueue.take();
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
