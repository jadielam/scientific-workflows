package io.biblia.workflows.manager.action;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Independent threaded class that runs every certain amount
 * of time (60 seconds) scraping the database to see if
 * there are actions that are available to be submitted.
 * @author jadiel
 *
 */
class ActionScraper {

	
	private static ActionScraper instance = null;
	
	/**
	 * The thread that runs this class
	 */
	private static Thread t;
	
	/**
	 * Queue to which actions to be submitted are added.
	 */
	private final BlockingQueue<PersistedAction> queue;
	
	/**
	 * Data access object that gets and updates actions
	 * in the database.
	 */
	private final ActionPersistance actionDao;
	
	/**
	 * Milliseconds to wait until getting available actions from
	 * the database again.
	 */
	private static final long ACTION_SCRAPER_TIMEOUT = 60000;
	
	private static final int QUEUE_SOFT_MAX_CAPACITY = 20;
	
	final Logger logger = LoggerFactory.getLogger(ActionScraper.class);
	
	private class ActionScraperRunner implements Runnable {

		@Override
		public void run() {
			
			logger.info("Started ActionScraper");
			System.out.println("Started ActionManager");
			while(!Thread.currentThread().isInterrupted()) {
				//1. Every certain amount of time you find available
				//actions from the database.
				//1.1 Actions that qualify are the following:
				// Actions that are not being processed yet
				// Actions that have been started processing, but have
				// been on that state for a long time. That could signal
				// that the server that started processing them died.
				int number = Math.max(QUEUE_SOFT_MAX_CAPACITY - queue.size(), 0);
				List<PersistedAction> actions = actionDao.getAvailableActions(number);
				
				logger.info("Scraped {} available actions from the database", actions.size());
				System.out.println("Scraped available actions from database");
				System.out.println(actions.size());
				//2. For each of the actions, update the entry of the
				//action in the database, if it is that it has not been
				//updated by someone else first.  If it has been updated
				//by someone else, drop it, otherwise, insert it into
				//the queue.
				for (PersistedAction pAction : actions) {
					
					try{
						pAction = actionDao.updateActionState(pAction, ActionState.PROCESSING);
						logger.debug("Changed the state of action {} to PROCESSING", pAction.get_id());
						System.out.println("Changed the state of action to PROCESSING");
					}
					catch(OutdatedActionException ex) {
						logger.debug("Could not change the state of action {} to PROCESSING because action was outdated", pAction.get_id());
						continue;
					}
					catch(Exception e) {
						logger.debug("Could not change the state of action {} to PROCESSING for unknown reason", pAction.get_id());
						continue;
					}
					queue.add(pAction);
				}
				
				try {
					Thread.sleep(ActionScraper.ACTION_SCRAPER_TIMEOUT);
				} catch (InterruptedException e) {
					
					Thread.currentThread().interrupt();
				}
			}
		}
		
	}
	
	private ActionScraper(BlockingQueue<PersistedAction> queue,
			ActionPersistance actionDao) {
		Preconditions.checkNotNull(queue);
		Preconditions.checkNotNull(actionDao);
		this.queue = queue;
		this.actionDao = actionDao;
		t = new Thread(new ActionScraperRunner(), "Action Scraper Thread");
		t.start();
	}

	/**
	 * Sends the interrupt signal to the thread running the ActionScraperRunner
	 */
	public static void stop() {
		if (null != t) {
			t.interrupt();
		}
		
	}

	/**
	 * Starts the ActionScraper that continualy runs.
	 * @param actionsQueue
	 * @param actionDao
     */
	public static void start(BlockingQueue<PersistedAction> actionsQueue,
			ActionPersistance actionDao) {
		if (null == instance) {
			instance = new ActionScraper(actionsQueue, actionDao);
		}
	};
}
