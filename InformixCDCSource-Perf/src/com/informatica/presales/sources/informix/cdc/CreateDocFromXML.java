package com.informatica.presales.sources.informix.cdc;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
//import org.w3c.dom.Element;
import java.io.File;

class CreateDocFromXML {
	String m_Filename;
	Document m_Document;
	String m_ParsedOutput = "";
	boolean m_Debug = false;

	CreateDocFromXML(String filename) {
		m_Filename = filename;
	}

	void printElements(NodeList nodes, String path, int childCount) {
		int length = nodes.getLength();
		//System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		for (int nodeIndex = 0; nodeIndex < length; nodeIndex++) {
			Node node = nodes.item(nodeIndex);
			short nodeType = node.getNodeType();
			String name = node.getNodeName();
			if (nodeType != Node.TEXT_NODE ) {
				if (node.hasChildNodes()) {
					printElements(node.getChildNodes(), path + "." + name, length);
				}
			} else {
				if (length == 1) {
					m_ParsedOutput += String.format("%s: <%s>\n", path, node.getTextContent());
				}
			}
		}
		//System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

	}

	void execute() {

		try {

			File fXmlFile = new File(m_Filename);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			m_Document = dBuilder.parse(fXmlFile);

			// optional, but recommended
			// read this -
			// http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
			//m_Document.getDocumentElement().normalize();

			System.out.println("Root element :" + m_Document.getDocumentElement().getNodeName());

			NodeList nodes = m_Document.getChildNodes();
			
			if (m_Debug) {
				printElements(nodes, m_Document.getNodeName(), nodes.getLength());
				System.out.println(m_ParsedOutput);
			}

			System.out.println("----------------------------");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
