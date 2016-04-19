package io.biblia.workflows.oozie;

import com.google.common.base.Preconditions;

public class FSDeleteAction extends OozieAction {

	private final String toDeletePath;
	
	public FSDeleteAction(String name, String okName, String errorName,
			String toDeletePath) {
		super(name, okName, errorName);
		Preconditions.checkNotNull(toDeletePath);
		this.toDeletePath = toDeletePath;
	}
	
	public String getPathToDelete() {
		return this.toDeletePath;
	}

}
