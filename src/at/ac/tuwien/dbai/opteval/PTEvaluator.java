package at.ac.tuwien.dbai.opteval;

import java.io.PrintStream;
import java.util.Iterator;

import at.ac.tuwien.dbai.sparql.query.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

public class PTEvaluator {
    static final Logger logger = LogManager.getLogger(PTEvaluator.class.getName());

    public static long startTime;

    public static void main(String[] args) throws Exception {
        startTime = System.nanoTime();
        XMLReader parser = XMLReaderFactory.createXMLReader();

        PTXmlHandler handler = new PTXmlHandler();
        parser.setContentHandler(handler);
        String mode = "";
        if (args.length == 2) {
            mode = args[0];
            parser.parse("resources/test.xml");
            printTime("XML");
        } else
            Util.error("Usage: PTEvaluator <pt.xml>");

        EvalPT pt = handler.getEvalPT();
        MaxMappingSet mappings = new MaxMappingSet();
        PrintStream outStream;
        MappingSet set;
        String outputFile;

        if (mode.equals("db")) {
            set = pt.evaluate(EvalPT.EvaluationType.DB);
            outputFile = "output/test-db.txt";
        } else {
            set = pt.evaluate(EvalPT.EvaluationType.ITERATIVE);
            outputFile = "output/test.txt";
        }
        outStream = new PrintStream(outputFile);
        printTime("evaluate");
        mappings.addAll(set);
        printTime("MaxSet");

        //Output
        int numberOfRows = 0;
        outStream.println();
        outStream.println("=======================================================================================");

        for (Mapping map : mappings) {
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

        printTime("Output");
    }

    public static void printTime(String event) {
        long estimatedTime = System.nanoTime() - startTime;
        double seconds = (double) estimatedTime / 1000000000.0;
        logger.info("TIME - " + event + ": " + seconds + " sec");
    }
}
