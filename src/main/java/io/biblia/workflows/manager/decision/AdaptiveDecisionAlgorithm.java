package io.biblia.workflows.manager.decision;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.math3.stat.StatUtils;
import com.google.common.primitives.Doubles;

import io.biblia.workflows.manager.dataset.PersistedDataset;

public class AdaptiveDecisionAlgorithm implements DecisionAlgorithm {

	@Override
	public List<String> toDelete(SimplifiedWorkflowHistory history, List<PersistedDataset> storedDatasets,
			Long spaceToFree) {
		
		Map<String, PersistedDataset> storedDatasetsMap = new HashMap<>();
		
		for (PersistedDataset d : storedDatasets) {
			storedDatasetsMap.put(d.getPath(), d);
		}
		List<String> toReturn = new ArrayList<>();
		
		//1. Create an inverted index from workflow to datasets
		SortedMap<Long, Set<String>> workflowToDatasets = new TreeMap<>();
		Set<String> datasets = history.getActions();
		for (String dataset : datasets) {
			List<Long> workflowsIds = history.getActionWorkflows(dataset);
			if (null != workflowsIds) {
				for (Long workflowId : workflowsIds) {
					if (!workflowToDatasets.containsKey(workflowId)) {
						workflowToDatasets.put(workflowId, new HashSet<String>());
					}
					workflowToDatasets.get(workflowId).add(dataset);
				}
			}
		}
		
		//2. Create a map from action to last workflow where it was 
		//seen
		//3. Create a recency list and add elements to it
		List<Double> recencyList = new ArrayList<>();
		Map<String, Long> datasetToLastWorkflow = new HashMap<>();
		Set<Entry<Long, Set<String>>> entrySet = workflowToDatasets.entrySet();
		for(Entry<Long, Set<String>> e : entrySet) {
			Long workflowId = e.getKey();
			Set<String> wDatasets = e.getValue();
			for (String dataset : wDatasets) {
				if (datasetToLastWorkflow.containsKey(dataset)) {
					Long lastWorkflowId = datasetToLastWorkflow.get(dataset);
					recencyList.add(new Double(workflowId - lastWorkflowId));
				}
				datasetToLastWorkflow.put(dataset, workflowId);
			}
		}
		
		//5. Find mean and std of recency list
		double [] numbers = Doubles.toArray(recencyList);
		double mean = StatUtils.mean(numbers);
		double std = Math.sqrt(StatUtils.variance(numbers));
		
		//6. Calculate value of each dataset with workflow id within mean + 3std of 
		//current time
		int a = (int)(mean + 3 * std);
		Map<String, Double> datasetToValue = new HashMap<>();
		SortedMap<Long, Set<String>> tailWorkflows = workflowToDatasets.tailMap(workflowToDatasets.lastKey() - a);
		for(Entry<Long, Set<String>> e : tailWorkflows.entrySet()) {
			Set<String> tDatasets = e.getValue();
			for (String dataset : tDatasets) {
				if (!datasetToValue.containsKey(dataset)) {
					datasetToValue.put(dataset, 1.0);
				}
				else {
					datasetToValue.put(dataset, datasetToValue.get(dataset) + 1);
				}
			}
		}
		
		ArrayList<SimpleEntry<String, Double>> values = new ArrayList<>();
		for (Entry<String, Double> e : datasetToValue.entrySet()) {
			values.add(new SimpleEntry<>(e.getKey(), e.getValue()));
		}
		Collections.sort(values, new Comparator<SimpleEntry<String, Double>>() {
			
			public int compare(final SimpleEntry<String, Double> e1,
					final SimpleEntry<String, Double> e2) {
				return e1.getValue().compareTo(e2.getValue());
			}
		});
		
		//7. Pick values until we fill the spaceToFree.
		long spaceFred = 0;	
		for (PersistedDataset dataset : storedDatasets) {
			if (!datasetToValue.containsKey(dataset.getPath())) {
				toReturn.add(dataset.getPath());
				spaceFred += dataset.getSizeInMB();
			}
			if (spaceFred > spaceToFree) {
				break;
			}
		}
		
		int i = 0; 
		while (i < values.size() && spaceFred < spaceToFree) {
			SimpleEntry<String, Double> e = values.get(i);
			String outputPath = e.getKey();
			
			if (storedDatasetsMap.containsKey(outputPath)) {
				ActionData actionData = history.getActionData(outputPath);
				double sizeInMB = actionData.getSizeInMB();
				spaceFred += sizeInMB;
				toReturn.add(outputPath);
				
			}
		}
		
		return toReturn;
	}

}
