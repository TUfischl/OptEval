package at.ac.tuwien.dbai.opteval;

import java.io.File;
import java.io.PrintStream;
import java.util.Iterator;

import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import at.ac.tuwien.dbai.sparql.query.EvalPT;
import at.ac.tuwien.dbai.sparql.query.Mapping;
import at.ac.tuwien.dbai.sparql.query.MappingSet;
import at.ac.tuwien.dbai.sparql.query.MaxMappingSet;
import at.ac.tuwien.dbai.sparql.query.PatternTree;
import at.ac.tuwien.dbai.sparql.query.TreeFactory;

public class PTEvaluator {

	public PTEvaluator() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws Exception {
		XMLReader parser = XMLReaderFactory.createXMLReader();
		
		PTXmlHandler handler = new PTXmlHandler();
		parser.setContentHandler(handler);
		
		if (args.length == 1 ) {
			parser.parse(args[0]);
		} else
			Util.error("Usage: PTEvaluator <pt.xml>");

		EvalPT pt = handler.getEvalPT();
		MaxMappingSet mappings = new MaxMappingSet();
		MappingSet m = pt.evaluate();
		mappings.addAll(m);
		
		//Output
		int numberOfRows = 0;
		PrintStream outStream = new PrintStream("test.out");
		
		outStream .println();
	    outStream.println("=======================================================================================");

	    for (Mapping map : mappings) {
		    int i = 0;
		    for (Iterator<String> it = map.domain().iterator(); it.hasNext(); ) {
			    String var = it.next();
			    outStream.print(var + " -> " + map.get(var));
			    if (it.hasNext()) outStream.print(" | ");
		    }
		    outStream.println();
		    ++numberOfRows;
	    }
	   
	   	   
	    outStream.println("---------------------------------------------------------------------------------------");
	    outStream.println("  The number of rows returned: " + numberOfRows + "(not maximal: " + m.size() + ")");
	    outStream.println("=======================================================================================");
	    outStream.println();
	}
}
