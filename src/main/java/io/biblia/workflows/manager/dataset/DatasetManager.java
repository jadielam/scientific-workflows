package io.biblia.workflows.manager.dataset;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Preconditions;

import java.util.concurrent.BlockingQueue;

public class DatasetManager {

	private static DatasetManager instance = null;
	
	private static Thread t;
	
	private static final BlockingQueue<PersistedDataset> datasetsQueue;
	
	private static final ExecutorService datasetDeletorsExecutor;
	
	private static final int NUMBER_OF_DATASET_DELETORS = 5;
	
	private DatasetPersistance datasetPersistance;
	
	static {
		
		//1. Create the concurrent queue
		datasetsQueue = new LinkedBlockingQueue<>();
		
		datasetDeletorsExecutor = 
				Executors.newFixedThreadPool(NUMBER_OF_DATASET_DELETORS);
		
	}
	
	private class DatasetManagerRunner implements Runnable {

		@Override
		public void run() {
			
			while (!Thread.currentThread().isInterrupted()) {
				try{
					PersistedDataset dataset = datasetsQueue.take();
					datasetDeletorsExecutor.execute(new DatasetDeletor(dataset, datasetPersistance));
					Thread.sleep(100);
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					
				}
			
			}
		
		}
	}
	
	private DatasetManager(DatasetPersistance datasetPersistance) {
		
		Preconditions.checkNotNull(datasetPersistance);
		this.datasetPersistance = datasetPersistance;
		
		DatasetScraper.start(datasetsQueue, datasetPersistance);
		
		t = new Thread(new DatasetManagerRunner(), "DatasetManager thread");
		t.start();
	}
	
	public static void start(DatasetPersistance persistance) {
		if (null == instance) {
			instance = new DatasetManager(persistance);
		}
	}
	
	public static void stop() {
		finishActionScraper();
		finishDatasetDeletors();
		t.interrupt();
	}
	
	private static void finishDatasetDeletors() {
		datasetDeletorsExecutor.shutdown();
	}
	
	private static void finishActionScraper() {
		DatasetScraper.stop();
	}
}