package io.biblia.workflows.definition;

import java.util.LinkedHashMap;
import java.util.List;

import org.bson.Document;

public interface Action {

	public String getOriginalName();
	
	public String getUniqueName();
	
	public List<String> getLongName();
	
	public String getOutputPath();
	
	public LinkedHashMap<String, String> getExtraInputs();
	
	public LinkedHashMap<String, String> getConfiguration();
	
	public ActionType getType();
	
	public boolean forceComputation();
	
	public List<String> getInputPaths();
	
	public Document toBson();
}
