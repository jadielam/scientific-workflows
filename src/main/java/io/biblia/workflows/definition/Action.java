package io.biblia.workflows.definition;

import java.util.List;
import java.util.Set;

public interface Action {

	public String getName();
	
	public Set<String> getParentActionNames();
	
	public List<String> getInputParameters();
	
	public List<String> getOutputParameters();
}
