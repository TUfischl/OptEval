package at.ac.tuwien.dbai.opteval;

import java.io.File;
import java.io.PrintStream;
import java.util.Iterator;

import at.ac.tuwien.dbai.db.H2Con;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import at.ac.tuwien.dbai.sparql.query.EvalPT;
import at.ac.tuwien.dbai.sparql.query.Mapping;
import at.ac.tuwien.dbai.sparql.query.MappingSet;
import at.ac.tuwien.dbai.sparql.query.MaxMappingSet;

public class PTEvaluator {

	public PTEvaluator() {
		// TODO Auto-generated constructor stub
	}

	public static long startTime;

	public static void main(String[] args) throws Exception {
		XMLReader parser = XMLReaderFactory.createXMLReader();
		
		PTXmlHandler handler = new PTXmlHandler();
		parser.setContentHandler(handler);
		String mode = "";
		if (args.length == 2 ) {
			mode = args[0];
			parser.parse(args[1]);
		} else
			Util.error("Usage: PTEvaluator <pt.xml>");


		EvalPT pt = handler.getEvalPT();
		MaxMappingSet mappings = new MaxMappingSet();
		PrintStream outStream = null;

		startTime = System.nanoTime();

		if (mode.equals("db")) {
			H2Con.dropTableIfExists("root");
			String[] rootCols = new String[]{"?X1", "?Y1"};
			H2Con.createTable("root", rootCols);
			H2Con.insertIntoTable("root", rootCols, pt.getRoot().getMappings());

			H2Con.dropTableIfExists("child1");
			String[] child = new String[]{"?Y1", "?Y2", "?X2"};
			H2Con.createTable("child1", child);
			H2Con.insertIntoTable("child1", child, pt.getRoot().getChildren().iterator().next().getMappings());
			String select = "SELECT X1, X2\n" +
					"FROM ROOT r\n" +
					"LEFT OUTER JOIN\n" +
					"CHILD1 c \n" +
					"on r.Y1=c.Y1;";
			MappingSet set = H2Con.select(select);
			printTime("Finished evaluate");
			mappings.addAll(set);
			outStream = new PrintStream("output/test-db.txt");
		} else {
			MappingSet set = pt.evaluate();
			printTime("Finished evaluate");
			mappings.addAll(set);
			outStream = new PrintStream("output/test.txt");
		}
		printTime("Finished MaxSet");

		//Output
		int numberOfRows = 0;
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
		outStream.println("  The number of rows returned: " + numberOfRows);
		outStream.println("=======================================================================================");
		outStream.println();

		printTime("Finished Output");
	}

	public static void printTime(String event) {
		long estimatedTime = System.nanoTime() - startTime;
		double seconds = (double)estimatedTime / 1000000000.0;
		System.out.println(event + ":\t\t\t" + seconds + " sec");
	}
}
