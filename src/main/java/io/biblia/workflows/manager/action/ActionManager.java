package io.biblia.workflows.manager.action;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Preconditions;

/**
 * The action manager's purpose is to submit actions to Hadoop, in this case
 * using Apache Oozie as intermediary.  It uses the database to synchronize
 * with other processes that might also be submitting actions to Hadoop. (Because
 * actions to be submitted to Hadoop will be saved in the dataset, and there will be
 * a scrapper getting new actions to submit from the database.  In order to not have 
 * multiple scrapers getting the same actions, we need that synchronization, which is
 * implemented by adding a state field to the actions, as well as versioning.)
 * 
 * It works as follows:
 * - It has an actions queue with the actions that need to be submitted.
 * - It has an actions scraper that queries the database for actions to 
 * be submitted. THe scraper fills the actions queue with actions
 * - The ActionManager takes new actions from the queue and hands them
 * to ActionSubmitter threads that will submit the actions to Oozie and 
 * update the database accordingly.
 * 
 * In order to support multiple servers doing the same process at the same time,
 * and to avoid the need of using a synchronization server such as Apache Zookeeper,
 * I have implemented synchronization in the following way:
 * 
 * 1. Actions can be in one of many states. To see the possible action states {@link ActionState}.
 * There are listed here too: READY, PROCESSING, SUBMITTED, RUNNING, FINISHED,
	FAILED, KILLED.
   
   2. The {@link ActionScraper} will find available actions in the database to start
   processing: Available actions are defined as actions that are in the READY state,
   or actions that have been in the PROCESSING state for a long time.  The reason
   to include these last kind of actions is the following: It well could be that
   another server started processing an action and then it died without being 
   able to change the state of that action to submitted.
   
   3. The {@link ActionManager} is constantly taking new elements from the queue and
   passing them to {@link ActionSubmitter} threads that take care of submitting the
   actions to Hadoop.  The decision of including old PROCESSING actions in the queue
   makes the design of the {@link ActionSubmitter} more careful.
   
   4. The {@link ActionSubmitter} will immediately attempt to mark an action as submitted
   in the database.  If it does not succeed, because the database contains a more recent
   version of the action, it immediately drops the action that it is working on.  Otherwise
   it continues with its course.
   
   5. A problem still without solution is the following: If an action is submitted to
   Hadoop, but the server that submitted the action goes down, then the callback
   notification endpoint of the submission of the action will not work and the database
   will not be updated (IMPORTANT: the callback should (will) work. All I need is a load balancer here).
   Because of that, another solution is having another thread
   constantly running and checking status of actions that have been submitted
   but that have not yet received a status of finished.  No, instead of that what I
   should do is provide Oozie with a list of callbacks, or with a virtual address
   callback which is a load balancer, and the balancer will send it to 
   currently running servers at that addresss.  This is a best solution, and more 
   natural.
 */
public class ActionManager {

	private static ActionManager instance = null;
	
	private static Thread t;
	
	private static final BlockingQueue<PersistedAction> actionsQueue;
	
	private static final ExecutorService actionSubmittersExecutor;
	
	private static final int NUMBER_OF_ACTION_SUBMITTERS = 5;
	
	private ActionPersistance actionPersistance;
	
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
					PersistedAction action = actionsQueue.take();
					actionSubmittersExecutor.execute(new ActionSubmitter(action, actionPersistance));
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
		
		this.actionPersistance = actionPersistance;
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
