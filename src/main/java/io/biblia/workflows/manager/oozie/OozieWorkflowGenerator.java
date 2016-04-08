package io.biblia.workflows.manager.oozie;

import java.util.LinkedList;
import java.util.List;

import io.biblia.workflows.EnvironmentVariables;
import io.biblia.workflows.definition.Action;
import io.biblia.workflows.utils.XmlBuilder;

public class OozieWorkflowGenerator implements EnvironmentVariables {

	/**
	 * Workflow version
	 */
	private static final String XMLNS = "uri:oozie:workflow:0.5";
	
	static final String DEFAULT_JOB_TRACKER; 
			
	static final String DEFAULT_NAME_NODE;
	
	static {
		String jobTracker = System.getenv().get(SW_HADOOP_JOBTRACKER);
		String nameNode = System.getenv().get(SW_HADOOP_NAMENODE);
		if (null == jobTracker) {
			jobTracker = "localhost:8032";
		}
		if (null == nameNode) {
			nameNode = "hdfs://localhost:8020";
		}
		DEFAULT_JOB_TRACKER = jobTracker;
		DEFAULT_NAME_NODE = nameNode;
	}
		
	/**
	 * Workflow names
	 */
	private String wName;
	
	private String startNodeName;
	
	private String endNodeName;
	
	private String killNodeName;
	
	private List<OozieAction> workflowActions = new LinkedList<OozieAction>();
	
	public OozieWorkflowGenerator() {
		
	}
	
	public OozieWorkflowGenerator addName(String name) {
		this.wName = name;
		return this;
	}
	
	public OozieWorkflowGenerator addStartNode(String actionName) {
		this.startNodeName = actionName;
		return this;
	}
	
	public OozieWorkflowGenerator addEndNode (String actionName) {
		this.endNodeName = actionName;
		return this;
	}
	
	public OozieWorkflowGenerator addKillNode(String nodeName) {
		this.killNodeName = nodeName;
		return this;
	}
	
	public OozieWorkflowGenerator addAction(OozieAction action) {
		this.workflowActions.add(action);
		return this;
	}
	
	/**
	 * Validates that the workflow is valid.
	 * @return
	 */
	private boolean isValid() {
		if (null == this.wName) return false;
		if (null == this.startNodeName) return false;
		if (null == this.endNodeName) return false;
		return true;
	}
	
	public String generateWorkflow() throws IllegalStateException {
		
		if (isValid()) {
			XmlBuilder builder = new XmlBuilder();
			builder.openElement("workflow-app", "xmlns", XMLNS, "name", this.wName);
			builder.openCloseElement("start", "to", this.startNodeName);
			for (OozieAction action : this.workflowActions) {
				builder.openElement("action", "name", action.getName());
				this.addActionSpecificValues(builder, action);
				builder.openCloseElement("ok", "to", action.getOkName());
				builder.openCloseElement("error", "to", action.getErrorName());
				builder.closeElement("action");
			}
			builder.openCloseElement("end", "name", this.endNodeName);
			if (null != this.killNodeName) {
				builder.openCloseElement("kill", "name", this.killNodeName);
			}
			builder.closeElement("workflow-app");
			return builder.toString();
		}
		else {
			throw new IllegalStateException();
		}
	}
	
	private void addActionSpecificValues(XmlBuilder builder, OozieAction action) {
		if (action instanceof MapReduceAction) {
			MapReduceAction mr = (MapReduceAction) action;
			builder.openElement("map-reduce");
			builder.openCloseTextElement("job-tracker", DEFAULT_JOB_TRACKER);
			builder.openCloseTextElement("name-node", DEFAULT_NAME_NODE);
			builder.openElement("configuration");
			this.addProperty(builder, "mapred.mapper.class", mr.getMapperClass());
			this.addProperty(builder, "mapred.reducer.class", mr.getReducerClass());
			this.addProperty(builder, "mapred.input.dir", mr.getInputDirectory());
			this.addProperty(builder, "mapred.output.dir", mr.getOutputDirectory());
			builder.closeElement("configuration");
			builder.closeElement("map-reduce");
			mr.getInputDirectory();
			mr.getOutputDirectory();
		}
		else if (action instanceof JavaAction) {
			JavaAction c = (JavaAction) action;
			builder.openElement("java");
			builder.openCloseTextElement("job-tracker", DEFAULT_JOB_TRACKER);
			builder.openCloseTextElement("name-node", DEFAULT_NAME_NODE);
			List<String> arguments = c.getArguments();
			for (String arg : arguments) {
				builder.openCloseTextElement("arg", arg);
			}
			builder.closeElement("java");
		}
	}
	
	private void addProperty(XmlBuilder builder, String name, String value) {
		builder.openElement("property");
		builder.openCloseTextElement("name", name);
		builder.openCloseTextElement("value", value);
		builder.closeElement("property");
	}
}
