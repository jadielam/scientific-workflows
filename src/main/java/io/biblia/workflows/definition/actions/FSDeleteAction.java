package io.biblia.workflows.definition.actions;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.bson.Document;

import com.google.common.base.Preconditions;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.parser.ActionNameConstants;

public class FSDeleteAction extends Action implements ActionNameConstants {
	
	private final String pathToDelete;
	
	public FSDeleteAction(String name, String pathToDelete) {
		super(name, FS_DELETE_ACTION, null, true);
		Preconditions.checkNotNull(pathToDelete);
		this.pathToDelete = pathToDelete;
	}
	
	public String getPathToDelete() {
		return this.pathToDelete;
	}

	@Override
	public Set<String> getParentActionNames() {
		return Collections.<String>emptySet();
	}

	@Override
	public Map<String, String> getInputParameters() {
		return Collections.<String, String>emptyMap();
	}

	@Override
	public Map<String, String> getOutputParameters() {
		return Collections.<String, String>emptyMap();
	}

	@Override
	public Map<String, String> getConfigurationParameters() {
		return Collections.<String, String>emptyMap();
	}
	
	public Document toBson() {
		Document toReturn = super.toBson();
		toReturn.append("pathToDelete", this.pathToDelete);
		return toReturn;
	}
}
