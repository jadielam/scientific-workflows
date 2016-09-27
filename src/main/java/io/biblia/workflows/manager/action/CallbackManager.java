package io.biblia.workflows.manager.action;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import java.util.List;

public class CallbackManager {

	private static CallbackManager instance = null;
	
	private static Thread t;
	
	private Callback callback;
	
	private static final BlockingQueue<String> queue;
	
	private ActionPersistance actionPersistance;
	
	final static Logger logger = LoggerFactory.getLogger(CallbackManager.class);
	
	static {
		queue = new LinkedBlockingQueue<>();
	}
	
	private class CallbackManagerRunner implements Runnable {
		
		@Override
		public void run() {
			logger.info("Started CallbackManager");
			SubmittedActionScraper.start(queue, actionPersistance);
			
		}
	}
	
	private CallbackManager(ActionPersistance aPersistance) {
		Preconditions.checkNotNull(aPersistance);
		
		this.actionPersistance = aPersistance;
		
		SubmittedActionScraper.start(queue, actionPersistance);
		
		t = new Thread(new CallbackManagerRunner(), "CallbackManager thread");
		
		t.start();
	}
	
	public static void start(ActionPersistance persistance) {
		if (null == instance) {
			instance = new CallbackManager(persistance);
		}
	}
	
	public static void join() {
		try {
			t.join();
		}
		catch(InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	public static void stop() {
		finishSubmittedActionScraper();
		if (null != t) {
			t.interrupt();
		}
	}
	
	private static void finishSubmittedActionScraper() {
		SubmittedActionScraper.stop();
	}
}

class SubmittedActionScraper {
	
	private static SubmittedActionScraper instance = null;
	
	private static Thread t;
	
	private final BlockingQueue<String> queue;
	
	private final ActionPersistance actionDao;
	
	private static final long SUBMITTED_ACTION_SCRAPER_TIMEOUT = 60000;
	
	final Logger logger = LoggerFactory.getLogger(SubmittedActionScraper.class);
	
	private class ActionScraperRunner implements java.lang.Runnable {
		
		@Override
		public void run() {
			
			while(!Thread.currentThread().isInterrupted()) {
				
				//1. Get all the submitted actions
				List<PersistedAction> actions =  actionDao.getSubmittedActions();
				
				//2. Place the action submitted id in the queue
				for (PersistedAction pAction : actions) {
					String submissionId = pAction.getSubmissionId();
					if (null != submissionId) {
						queue.add(submissionId);
					}
					else {
						logger.error("Could not add action {} to the queue because it had null submissionID", pAction.get_id());
					}
				}
				
				try {
					Thread.sleep(SubmittedActionScraper.SUBMITTED_ACTION_SCRAPER_TIMEOUT);
				}
				catch(InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
	
	private SubmittedActionScraper(BlockingQueue<String> queue, ActionPersistance actionDao) {
		Preconditions.checkNotNull(queue);
		Preconditions.checkNotNull(actionDao);
		this.queue = queue;
		this.actionDao = actionDao;
		t = new Thread(new ActionScraperRunner(), "Submitted Action Scraper Thread");
		t.start();
	}
	
	public static void stop() {
		if (null != t) {
			t.interrupt();
		}
	}
	
	public static void start(BlockingQueue<String> actionsQueue, ActionPersistance actionDao) {
		if (null == instance){
			instance = new SubmittedActionScraper(actionsQueue, actionDao);
		}
			
	}
}
