package io.biblia.workflows.definition;

public class InvalidWorkflowException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4884314399198525395L;

	public InvalidWorkflowException(String explanation) {
		super(explanation);
	}
}
