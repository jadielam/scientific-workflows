package io.biblia.workflows.manager.oozie;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class JavaAction extends OozieAction {

	private final String mainClass;
	
	private final List<String> arguments;
	
	public JavaAction(String name, String okName, String errorName,
			String mainClass, List<String> arguments) {
		super(name, okName, errorName);
		Preconditions.checkNotNull(mainClass);
		Preconditions.checkNotNull(arguments);
		this.mainClass = mainClass;
		this.arguments = ImmutableList.<String>builder().addAll(arguments).build();
	}

	public String getMainClass() {
		return mainClass;
	}

	public List<String> getArguments() {
		return arguments;
	}

}
