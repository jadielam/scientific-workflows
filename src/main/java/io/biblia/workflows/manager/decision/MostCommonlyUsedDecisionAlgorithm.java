package io.biblia.workflows.manager.decision;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.LinkedList;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;

public class MostCommonlyUsedDecisionAlgorithm implements DecisionAlgorithm {

	@Override
	public List<String> toDelete(SimplifiedWorkflowHistory workflowHistory, 
			List<String> storedDatasets, Long spaceToFree) {
		
		//1. Get the keys of actions in the window.
		Collection<String> actions = workflowHistory.getActions();
		
		//2. Get the count for each action
		ArrayList<SimpleEntry<String, Integer>> counts = new ArrayList<>();
		for (String action : actions) {
			Integer count = workflowHistory.getActionCount(action);
			if (null != count) {
				counts.add(new SimpleEntry<>(action, count));
			}
		}
		
		//3. Sort the actions by count, ascending
		Collections.sort(counts, new Comparator<SimpleEntry<String, Integer>>() {
			
			@Override
			public int compare(final SimpleEntry<String, Integer> e1, 
					final SimpleEntry<String, Integer> e2) {
				return e1.getValue().compareTo(e2.getValue());
			}
		
		});
		
		//4. Starting from the beginning of the sorted list, add actions to
		//the delete list until equaling or exceeding spaceToFree.
		long spaceFred = 0;
		int i = 0;
		List<String> outputsToDelete = new LinkedList<>();
		while (i < counts.size() && spaceFred < spaceToFree) {
			SimpleEntry<String, Integer> e = counts.get(i);
			String outputPath = e.getKey();
			ActionData actionData = workflowHistory.getActionData(outputPath);
			double sizeInMB = actionData.getSizeInMB();
			spaceFred += sizeInMB;
			outputsToDelete.add(outputPath);
			i++;
		}
		
		//5. Return the list.
		return outputsToDelete;
	}

}
