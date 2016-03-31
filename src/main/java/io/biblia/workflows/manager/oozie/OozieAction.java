package io.biblia.workflows.manager.oozie;

import com.google.common.base.Preconditions;

public abstract class OozieAction {

	private final String name;
	private final String okName;
	private final String errorName;
	
	public OozieAction(String name, String okName, String errorName) {
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(okName);
		Preconditions.checkNotNull(errorName);
		this.name = name;
		this.okName = okName;
		this.errorName = errorName;
	}

	public String getName() {
		return name;
	}

	public String getOkName() {
		return okName;
	}

	public String getErrorName() {
		return errorName;
	}
	
	
}
