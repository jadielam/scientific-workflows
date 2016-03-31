package io.biblia.workflows.utils;

public class TestXmlBuilder {

	public static void main(String[] args) {
		
		XmlBuilder builder = new XmlBuilder();
		builder.openElement("workflow");
		builder.openElement("a", "size", "2");
		builder.closeElement("a");
		builder.openCloseTextElement("text", "asdf", "a", "a");
		builder.openCloseElement("testing");
		builder.closeElement("workflow");
		
		System.out.println(builder.toString());

	}

}
