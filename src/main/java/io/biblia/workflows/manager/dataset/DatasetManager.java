package io.biblia.workflows.manager.dataset;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import java.util.logging.Level;

import io.biblia.workflows.manager.decision.DatasetLogDao;
import com.google.common.base.Preconditions;

import java.util.concurrent.BlockingQueue;

/**
 * It sets to run the dataset scraper and the dataset deletors.
 * The dataset scraper is constantly checking for datasets
 * marked TO_DELETE in the database to fill the queue of datasets
 * to be deleted.  The Dataset deletors are consumers that
 * take those datasets from the queue and delete them.
 * @author dearj019
 *
 */
public class DatasetManager {

	private static DatasetManager instance = null;
	
	private static Thread t;
	
	private static final BlockingQueue<PersistedDataset> datasetsQueue;
	
	private static final ExecutorService datasetDeletorsExecutor;
	
	private static final int NUMBER_OF_DATASET_DELETORS = 5;
	
	private static final Logger logger = Logger.getLogger(DatasetManager.class.getName());
	
	private DatasetPersistance datasetPersistance;
	
	private DatasetLogDao datasetLogDao;
	
	static {
		
		//1. Create the concurrent queue
		datasetsQueue = new LinkedBlockingQueue<>();
		
		datasetDeletorsExecutor = 
				Executors.newFixedThreadPool(NUMBER_OF_DATASET_DELETORS);
		
	}
	
	private class DatasetManagerRunner implements Runnable {

		@Override
		public void run() {
			
			logger.info("Started DatasetManager");
			while (!Thread.currentThread().isInterrupted()) {
				try{
					PersistedDataset dataset = datasetsQueue.take();
					logger.log(Level.FINER, "Took dataset {0} to the deletion queue", dataset.getPath());
					datasetDeletorsExecutor.execute(new DatasetDeletor(dataset, datasetPersistance, datasetLogDao));
					Thread.sleep(100);
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					
				}
			
			}
		
		}
	}
	
	private DatasetManager(DatasetPersistance datasetPersistance, DatasetLogDao datasetLogDao) {
		
		Preconditions.checkNotNull(datasetPersistance);
		Preconditions.checkNotNull(datasetLogDao);
		this.datasetPersistance = datasetPersistance;
		this.datasetLogDao = datasetLogDao;
		
		DatasetScraper.start(datasetsQueue, datasetPersistance);
		
		t = new Thread(new DatasetManagerRunner(), "DatasetManager thread");
		t.start();
	}
	
	public static void start(DatasetPersistance persistance, DatasetLogDao datasetLogDao) {
		if (null == instance) {
			instance = new DatasetManager(persistance, datasetLogDao);
		}
	}
	
	public static void join() {
		try{
			t.join();
		}
		catch(InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
		
	}
	public static void stop() {
		logger.log(Level.INFO, "Shutting down Dataset Manager");
		finishActionScraper();
		finishDatasetDeletors();
		if (null != t) {
			t.interrupt();
		}
		
	}
	
	private static void finishDatasetDeletors() {
		datasetDeletorsExecutor.shutdown();
	}
	
	private static void finishActionScraper() {
		DatasetScraper.stop();
	}
}
