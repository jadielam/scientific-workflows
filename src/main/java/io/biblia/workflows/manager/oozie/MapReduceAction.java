package io.biblia.workflows.manager.oozie;

import com.google.common.base.Preconditions;

public class MapReduceAction extends OozieAction {

	private final String inputDirectory;
	
	private final String outputDirectory;
	
	private final String mapperClass;
	
	private final String reducerClass;
	
	public MapReduceAction(String name, String okName, String errorName,
			String inputDirectory, String outputDirectory,
			String mapperClass, String reducerClass) {
		super(name, okName, errorName);
		Preconditions.checkNotNull(inputDirectory);
		Preconditions.checkNotNull(outputDirectory);
		Preconditions.checkNotNull(mapperClass);
		Preconditions.checkNotNull(reducerClass);
		this.inputDirectory = inputDirectory;
		this.outputDirectory = outputDirectory;
		this.mapperClass = mapperClass;
		this.reducerClass = reducerClass;
	}

	public String getInputDirectory() {
		return inputDirectory;
	}

	public String getOutputDirectory() {
		return outputDirectory;
	}

	public String getMapperClass() {
		return mapperClass;
	}

	public String getReducerClass() {
		return reducerClass;
	}
	

}
