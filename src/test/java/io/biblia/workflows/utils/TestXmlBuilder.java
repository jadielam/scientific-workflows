package io.biblia.workflows.utils;

import java.util.List;

import org.bson.Document;

import io.biblia.workflows.manager.action.ActionState;

public class TestXmlBuilder {

	public static void main(String[] args) {

		String json = "{ test: \"asdf\", test1: [{ a: 4}, {a:5}]}";
		Document parse = Document.parse(json);
		List<Document> inside = (List<Document>) parse.get("test1");
		
		System.out.println(ActionState.READY.name());

	}

}
