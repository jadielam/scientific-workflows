package io.biblia.workflows.manager;

import io.biblia.workflows.definition.Action;

public class OozieWorkflowGenerator {

	/**
	 * Workflow version
	 */
	private static final String XMLNS = "uri:oozie:workflow:0.5";
	
	/**
	 * Workflow String Builder
	 */
	private final StringBuilder wSB = new StringBuilder();
	
	/**
	 * Workflow names
	 */
	private String wName;
	
	public OozieWorkflowGenerator() {
		
	}
	
	public OozieWorkflowGenerator addName(String name) {
		this.wName = name;
		return this;
	}
	
	public OozieWorkflowGenerator addStartAction(String actionName) {
		return this;
	}
	
	public OozieWorkflowGenerator addEndAction(String actionName) {
		return this;
	}
	
	public OozieWorkflowGenerator addAction(Action action) {
		return this;
	}
	
	/**
	 * Validates that the workflow is valid.
	 * @return
	 */
	private boolean isValid() {
		if (this.wName == null) return false;
		return true;
	}
	
	public String generateWorkflow() {
		if (isValid()) {
			this.appendHeader();
			
			this.appendFooter();
			return wSB.toString();
		}
		else {
			throw new IllegalStateException();
		}
	}
	
	private void appendHeader() {
		wSB.append("<workflow-app xmlns=");
		wSB.append("\"").append(XMLNS).append("\"");
		wSB.append(" name=\"").append(this.wName).append("\"");
	}
	
	private void appendFooter() {
		
	}
}
