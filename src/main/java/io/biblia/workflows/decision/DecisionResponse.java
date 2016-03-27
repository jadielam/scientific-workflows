package io.biblia.workflows.decision;

import java.util.Collections;
import java.util.Set;

import io.biblia.workflows.definition.Dataset;

public class DecisionResponse {

	private final Set<Dataset> toKeep;
	
	private final Set<Dataset> toRemove;
	
	public DecisionResponse(Set<Dataset> toKeep, Set<Dataset> toRemove) {
		this.toKeep = Collections.unmodifiableSet(toKeep);
		this.toRemove = Collections.unmodifiableSet(toRemove);
	}
	
	
}
