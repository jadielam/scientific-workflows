package io.biblia.workflows.manager.action;

import io.biblia.workflows.oozie.OozieClientUtil;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.google.common.base.Preconditions;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob.Status;
import org.bson.types.ObjectId;

import java.util.List;

public class CallbackManager {

	private static CallbackManager instance = null;
	
	private static Thread t;
	
	private Callback callback;
	
	private static final BlockingQueue<PersistedAction> queue;
	
	private ActionPersistance actionPersistance;
	
	final static Logger logger = Logger.getLogger(CallbackManager.class.getName());
	
	static {
		queue = new LinkedBlockingQueue<>();
	}
	
	private class CallbackManagerRunner implements Runnable {
		
		@Override
		public void run() {
			logger.info("Started CallbackManager.");
			SubmittedActionScraper.start(queue, actionPersistance);
			while(!Thread.currentThread().isInterrupted()) {
				try {
					PersistedAction pAction = queue.take();
					logger.log(Level.FINE, "Processing action {0} from the submitted actions queue", pAction.get_id());
					String oozieSubmissionId = pAction.getSubmissionId();
					
					try {
						Status status = OozieClientUtil.getOozieWorkflowStatus(oozieSubmissionId);
						logger.log(Level.FINE, "Action " + pAction.get_id() + " has status {0}", status.name());
						if (Status.SUCCEEDED.equals(status)) {
							callback.actionFinished(pAction);
						}
						else if (Status.FAILED.equals(status)) {
							callback.actionFailed(pAction);
						}
						else if (Status.KILLED.equals(status)) {
							callback.actionKilled(pAction);
						}
						
						Thread.sleep(100);
					}
					catch (OozieClientException ex) {
						continue;
					}
					
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			
			
		}
	}
	
	private CallbackManager(ActionPersistance aPersistance, Callback callback) {
		Preconditions.checkNotNull(aPersistance);
		
		this.actionPersistance = aPersistance;
		this.callback = callback;
		
		t = new Thread(new CallbackManagerRunner(), "CallbackManager thread");
		
		t.start();
	}
	
	public static void start(ActionPersistance persistance, Callback callback) {
		if (null == instance) {
			instance = new CallbackManager(persistance, callback);
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
	
	private final BlockingQueue<PersistedAction> queue;
	
	private final ActionPersistance actionDao;
	
	private static final long SUBMITTED_ACTION_SCRAPER_TIMEOUT = 10000;
	
	final Logger logger = Logger.getLogger(SubmittedActionScraper.class.getName());
	
	private class ActionScraperRunner implements java.lang.Runnable {
		
		@Override
		public void run() {
			
			while(!Thread.currentThread().isInterrupted()) {
				
				//1. Get all the submitted actions
				List<PersistedAction> actions =  actionDao.getSubmittedActions();
				logger.log(Level.FINE, "Obtained {0} actions from the submitted actions queue", actions.size());;
				
				//2. Place the action submitted id in the queue
				for (PersistedAction pAction : actions) {
					if (null != pAction) {
						queue.add(pAction);
						logger.log(Level.FINE, "Added action {0} to the submitted actions queue", pAction.get_id());
					}
					else {
						logger.log(Level.SEVERE, "Could not add action {0} to the queue because it had null submissionID", pAction.get_id());
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
	
	private SubmittedActionScraper(BlockingQueue<PersistedAction> queue, ActionPersistance actionDao) {
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
	
	public static void start(BlockingQueue<PersistedAction> actionsQueue, ActionPersistance actionDao) {
		if (null == instance){
			instance = new SubmittedActionScraper(actionsQueue, actionDao);
		}
			
	}
}
