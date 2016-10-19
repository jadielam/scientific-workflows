package io.biblia.workflows.manager.decision;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import io.biblia.workflows.manager.dataset.PersistedDataset;

public class MostCommonlyUsedDecisionAlgorithm implements DecisionAlgorithm {

	@Override
	public List<String> toDelete(SimplifiedWorkflowHistory workflowHistory, 
			List<PersistedDataset> storedDatasets, Long spaceToFree) {
		
		Map<String, PersistedDataset> storedDatasetsMap = new HashMap<>();
		
		for (PersistedDataset d : storedDatasets) {
			storedDatasetsMap.put(d.getPath(), d);
		}
		
		//1. Get the keys of actions in the window.
		Set<String> actions = workflowHistory.getActions();

		//2. Get the count for each action
		ArrayList<SimpleEntry<String, Double>> counts = new ArrayList<>();
		for (String action : actions) {
			Integer count = workflowHistory.getActionCount(action);
			if (null != count) {
				counts.add(new SimpleEntry<>(action, new Double(count)));
			}
		}
		
		//3. Sort the actions by count, ascending
		Collections.sort(counts, new Comparator<SimpleEntry<String, Double>>() {
			
			@Override
			public int compare(final SimpleEntry<String, Double> e1, 
					final SimpleEntry<String, Double> e2) {
				return e1.getValue().compareTo(e2.getValue());
			}
		
		});
		
		long spaceFred = 0;
		List<String> outputsToDelete = new LinkedList<>();
		//4. Starting from the beginning of the storedDatasets list,
		//if the stored dataset path is not in the set of datasets of
		//the simplified workflow history, add it to the datasets to remove
		for (PersistedDataset d : storedDatasets) {
			if (!actions.contains(d.getPath())) {
				double sizeInMB = d.getSizeInMB();
				spaceFred += sizeInMB;
				outputsToDelete.add(d.getPath());
			}
			if (spaceFred > spaceToFree) {
				break;
			}
		}
		
		//4. Starting from the beginning of the sorted list, add actions to
		//the delete list until equaling or exceeding spaceToFree.
		int i = 0;
		while (i < counts.size() && spaceFred < spaceToFree) {
			SimpleEntry<String, Double> e = counts.get(i);
			String outputPath = e.getKey();
			
			//This means that the action is in stored state.
			if (storedDatasetsMap.containsKey(outputPath)) {
				ActionData actionData = workflowHistory.getActionData(outputPath);
				double sizeInMB = actionData.getSizeInMB();
				spaceFred += sizeInMB;
				outputsToDelete.add(outputPath);
			}
			
			i++;
		}
		
		//5. Return the list.
		return outputsToDelete;
	}

}
