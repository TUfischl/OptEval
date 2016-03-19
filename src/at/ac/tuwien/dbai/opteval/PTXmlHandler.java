package at.ac.tuwien.dbai.opteval;

import java.util.Stack;

import org.apache.commons.lang3.StringEscapeUtils;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import at.ac.tuwien.dbai.sparql.query.EvalPT;
import at.ac.tuwien.dbai.sparql.query.EvalTreeNode;
import at.ac.tuwien.dbai.sparql.query.Mapping;

public class PTXmlHandler extends DefaultHandler {
    private EvalPT pt;
    private Stack<EvalTreeNode> helperStack;
    private String currentString;
    private String currentVar;
    private Mapping currentMapping;
    private int nodeCount;
	
	public PTXmlHandler() {
		pt = new EvalPT();
		helperStack = new Stack<>();
        nodeCount = 0;
	}

	public void characters(char[] text, int start, int length) throws SAXException {
		currentString += new String(text, start, length);
	}

	public void endDocument() throws SAXException {
		if (helperStack.size() > 0) Util.error("Parsing error!");
	}
	
	public void startElement(String namespaceURI, String localName, String qualifiedName, Attributes atts) throws SAXException {
		if ("ovar".equals(localName)) currentString = "";
		
		if ("node".equals(localName)) {
			EvalTreeNode n = new EvalTreeNode(nodeCount);
            nodeCount++;
			helperStack.push(n);
		}
		
		if ("mapping".equals(localName)) currentMapping = new Mapping();
		
		if ("var".equals(localName)) {
			currentString = "";
			currentVar = atts.getValue("name");
		}

        if ("nodeVar".equals(localName)) currentString = "";
	}

	public void endElement(String namespaceURI, String localName, String qualifiedName) throws SAXException {
		if ("ovar".equals(localName)) pt.addOutputVar(currentString);
		
		if ("node".equals(localName)) {
			EvalTreeNode n = helperStack.pop();
			if (helperStack.isEmpty()) 
			   pt.setRoot(n);
			else
			   helperStack.peek().addChildNode(n);
		}
		
		if ("mapping".equals(localName)) helperStack.peek().addMapping(new Mapping(currentMapping));
		
		if ("var".equals(localName)) currentMapping.add(currentVar, StringEscapeUtils.unescapeXml(currentString));

        if ("nodeVar".equals(localName)) helperStack.peek().getLocalVars().add(currentString);
	}

	public EvalPT getEvalPT() {
		return pt;
	}

}
