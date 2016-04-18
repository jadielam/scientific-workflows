package io.biblia.workflows.manager.dataset;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.google.common.base.Preconditions;

public class DatasetScraper {

	private static DatasetScraper instance = null;
	
	private static Thread t;
	
	private final BlockingQueue<PersistedDataset> queue;
	
	private final DatasetPersistance datasetDao;
	
	private static final long DATASET_SCRAPER_TIMEOUT = 60000;
	
	private static final int QUEUE_SOFT_MAX_CAPACITY = 20;
	
	private class DatasetScraperRunner implements Runnable {

		@Override
		public void run() {
			
			while (!Thread.currentThread().isInterrupted()) {
				
				int number = Math.max(QUEUE_SOFT_MAX_CAPACITY - queue.size(), 0);
				List<PersistedDataset> datasets = datasetDao.getDatasetsToDelete(number);
				
				for (PersistedDataset pDataset : datasets) {
					try {
						pDataset = datasetDao.updateDatasetState(pDataset, DatasetState.PROCESSING);
					}
					catch(OutdatedDatasetException ex) {
						continue;
					}
					catch(Exception ex) {
						continue;
					}
					queue.add(pDataset);
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
		t.interrupt();
	}
	
	public static void start(BlockingQueue<PersistedDataset> datasetsQueue,
			DatasetPersistance datasetDao) {
		if (null == instance) {
			instance = new DatasetScraper(datasetsQueue, datasetDao);
		}
	}
}
