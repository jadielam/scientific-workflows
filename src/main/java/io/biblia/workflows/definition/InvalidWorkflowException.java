package io.biblia.workflows.definition;

/**
 * Exception to be thrown whenever the workflow is invalid.
 * A workflow can be invalid for multiple reasons, among them:
 * 1. If it contains a cycle.
 * 2. If an action is referenced without being defined.
 * 3. etc.
 * @author jadiel
 *
 */
public class InvalidWorkflowException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4884314399198525395L;

	public InvalidWorkflowException(String explanation) {
		super(explanation);
	}
}
