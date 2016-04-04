package io.biblia.workflows.manager.action;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.google.common.base.Preconditions;

/**
 * Independent threaded class that runs every certain amount
 * of time (60 seconds) scraping the database to see if
 * there are actions that are available to be submitted.
 * @author jadiel
 *
 */
class ActionScraper implements Runnable {

	
	private static ActionScraper instance = null;
	
	/**
	 * The thread that runs this class
	 */
	private Thread t;
	
	/**
	 * Queue to which actions to be submitted are added.
	 */
	private final BlockingQueue<ProcessingAction> queue;
	
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
	
	private ActionScraper(BlockingQueue<ProcessingAction> queue,
			ActionPersistance actionDao) {
		Preconditions.checkNotNull(queue);
		Preconditions.checkNotNull(actionDao);
		this.queue = queue;
		this.actionDao = actionDao;
		t = new Thread(this, "Scraper Thread");
		t.start();
	}
	
	@Override
	public void run() {
		
		while(true) {
			//1. Every certain amount of time you find available
			//actions from the database.
			//1.1 Actions that qualify are the following:
			// Actions that are not being processed yet
			// Actions that have been started processing, but have
			// been on that state for a long time. That could signal
			// that the server that started processing them died.
			List<ProcessingAction> actions = this.actionDao.getAvailableActions();
			
			//2. For each of the actions, update the entry of the
			//action in the database, if it is that it has not been
			//updated by someone else first.  If it has been updated
			//by someone else, drop it, otherwise, insert it into
			//the queue.
			for (ProcessingAction action : actions) {
				try{
					this.actionDao.updateProcessingAction(action);
				}
				catch(OutdatedActionException ex) {
					continue;
				}
				
				this.queue.add(action);
			}
			
			try {
				Thread.sleep(ActionScraper.ACTION_SCRAPER_TIMEOUT);
			} catch (InterruptedException e) {
				//Do nothing, just keep doing what you know to do:
				//scrape the database for more jobs.
			}
		}
	}

	public static void start(BlockingQueue<ProcessingAction> actionsQueue,
			ActionPersistance actionDao) {
		if (null == instance) {
			instance = new ActionScraper(actionsQueue, actionDao);
		}
	};
}
