package io.biblia.workflows.manager.dataset;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.google.common.base.Preconditions;

/**
 * Singleton instance that runs on its own thread and that is constantly
 * checking for new datasets to delete.
 * @author dearj019
 *
 */
public class DatasetScraper {

	private static DatasetScraper instance = null;
	
	private static Thread t;
	
	private final BlockingQueue<PersistedDataset> queue;
	
	private final DatasetPersistance datasetDao;
	
	private static final long DATASET_SCRAPER_TIMEOUT = 60000;
	
	private static final int QUEUE_SOFT_MAX_CAPACITY = 20;
	
	private static final Logger logger = Logger.getLogger(DatasetScraper.class.getName());
	
	private class DatasetScraperRunner implements Runnable {

		@Override
		public void run() {
			
			logger.info("Started Dataset Scraper");
			while (!Thread.currentThread().isInterrupted()) {
				
				int number = Math.max(QUEUE_SOFT_MAX_CAPACITY - queue.size(), 0);
				List<PersistedDataset> datasets = datasetDao.getDatasetsToDelete(number);
				logger.log(Level.FINE, "Obtained {0} datasets to delete from the database", datasets.size());
				
				for (PersistedDataset pDataset : datasets) {
					try {
						pDataset = datasetDao.updateDatasetState(pDataset, DatasetState.PROCESSING);
						logger.log(Level.FINE, "Updated state of dataset {0} to PROCESSING", pDataset.getPath());
					}
					catch(OutdatedDatasetException ex) {
						logger.log(Level.WARNING, "Could not update state of dataset {0} to PROCESSING because an OutdatedDatasetException was thrown", pDataset.getPath());
						continue;
					}
					catch(Exception ex) {
						logger.log(Level.SEVERE, "Could not update state of dataset {0} to PROCESSING because an unknown exception was thrown", ex);
						continue;
					}
					queue.add(pDataset);
					logger.log(Level.FINE, "Added dataset {0} to queue", pDataset.getPath());
				}
				
				try {
					Thread.sleep(DatasetScraper.DATASET_SCRAPER_TIMEOUT);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			
		}
		
	}
	
	private DatasetScraper(BlockingQueue<PersistedDataset> queue,
			DatasetPersistance datasetDao) {
		Preconditions.checkNotNull(queue);
		Preconditions.checkNotNull(datasetDao);
		this.queue = queue;
		this.datasetDao = datasetDao;
		t = new Thread(new DatasetScraperRunner(), "Dataset Scraper Thread");
		t.start();
	}
	
	public static void stop() {
		logger.info("Stopping DatasetScraper...");
		if (null != t) {
			t.interrupt();
		}
	}
	
	public static void start(BlockingQueue<PersistedDataset> datasetsQueue,
			DatasetPersistance datasetDao) {
		if (null == instance) {
			instance = new DatasetScraper(datasetsQueue, datasetDao);
		}
	}
}
