package io.biblia.workflows.utils;

import java.util.Stack;

import com.google.common.base.Preconditions;

public class XmlBuilder {

	private final StringBuilder sb = new StringBuilder();
	private final Stack<String> stack = new Stack<>();
	
	public XmlBuilder() {
		
	}
	
	public XmlBuilder openElement(String elementName, String ... arguments) 
			throws IllegalArgumentException {
		Preconditions.checkNotNull(elementName);
		if (arguments.length % 2 != 0) {
			throw new IllegalArgumentException();
		}
		this.sb.append(new String(new char[stack.size()]).replace("\0", "\t"));
		this.stack.push(elementName);
		this.sb.append("<").append(elementName);
		
		for (int i = 0, limit = arguments.length / 2; i < limit ; ++i) {
			String key = arguments[2 * i];
			String value = arguments[2 * i + 1];
			this.sb.append(" ");
			this.sb.append(key).append("=");
			this.sb.append("\"").append(value);
			this.sb.append("\"");
		}
		this.sb.append(">\n");
		return this;
	}
	
	public XmlBuilder openCloseTextElement(String elementName, String text, String ...arguments) {
		Preconditions.checkNotNull(text);
		if (this.stack.size() == 0) {
			throw new IllegalStateException("Text needs to be enclosed within an entity");
		}
		if (arguments.length % 2 != 0) {
			throw new IllegalArgumentException();
		}
		this.sb.append(new String(new char[stack.size()]).replace("\0", "\t"));
		this.sb.append("<").append(elementName);
		for (int i = 0, limit = arguments.length / 2; i < limit; ++i) {
			String key = arguments[2 * i];
			String value = arguments[2 * i + 1];
			this.sb.append(" ").append(key).append("=");
			this.sb.append("\"").append(value).append("\"");
		}
		this.sb.append(">").append(text);
		this.sb.append("</").append(elementName).append(">\n");
		return this;
	}
	public XmlBuilder closeElement(String elementName) {
		Preconditions.checkNotNull(elementName);
		String element = this.stack.pop();
		this.sb.append(new String(new char[stack.size()]).replace("\0", "\t"));
		if (!element.equals(elementName)) {
			throw new IllegalStateException("Element " + element + " needs "
					+ "to be closed first");
		}
		this.sb.append("</").append(elementName).append(">\n");
		return this;
	}
	
	public XmlBuilder openCloseElement(String elementName, String ... arguments) 
			throws IllegalArgumentException {
		Preconditions.checkNotNull(elementName);
		if (arguments.length % 2 != 0) {
			throw new IllegalArgumentException();
		}
		this.sb.append(new String(new char[stack.size()]).replace("\0", "\t"));
		this.sb.append("<").append(elementName);
		for (int i = 0, limit = arguments.length / 2; i < limit; ++i) {
			String key = arguments[2 * i];
			String value = arguments[2 * i + 1];
			this.sb.append(" ").append(key).append("=");
			this.sb.append("\"").append(value).append("\"");
		}
		this.sb.append("/>\n");		
		return this;
	}
	
	@Override
	public String toString() throws IllegalStateException {
		if (!this.stack.empty()) {
			throw new IllegalStateException("The xml has elements that have not been closed.");
		}
		return sb.toString();
	}
}
