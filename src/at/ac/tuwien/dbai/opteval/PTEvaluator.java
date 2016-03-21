package at.ac.tuwien.dbai.opteval;

import java.io.PrintStream;
import java.util.Iterator;

import at.ac.tuwien.dbai.db.DBConnection;
import at.ac.tuwien.dbai.db.DBMetaData;
import at.ac.tuwien.dbai.sparql.query.*;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

public class PTEvaluator {
    static final Logger logger = LogManager.getLogger(PTEvaluator.class.getName());
    private long startTime;
    private String[] args = null;
    private Options options = new Options();


    public static void main(String[] args) throws Exception {
        new PTEvaluator(args).parse();
    }

    public PTEvaluator(String[] args) {
        this.args = args;

        options.addOption("db", true, "use In-Memory-DB with type eg. 'h2'");
        options.addOption("i", "input", true, "use this xml file as input");
        options.addOption("o", "output", true, "use this file as output");
        options.addOption("h", "help", false, "prints help");
    }

    public void parse() throws Exception {
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine cmd = commandLineParser.parse(options, args);
        if (cmd.hasOption("h")) help();

        startTime = System.nanoTime();
        XMLReader parser = XMLReaderFactory.createXMLReader();

        PTXmlHandler handler = new PTXmlHandler();
        parser.setContentHandler(handler);
        EvalPT.EvaluationType evaluationType;
        DBMetaData.DBType dbType;
        String inputFilePath = "resources/test.xml";
        String outputFilePath;

        if (cmd.hasOption("db")) {
            outputFilePath = "output/test-db.txt";
            evaluationType = EvalPT.EvaluationType.DB;
            String db = cmd.getOptionValue("db").toUpperCase();
            try {
                dbType = DBMetaData.DBType.valueOf(db);
                DBConnection.setMetaData(dbType);
            } catch (IllegalArgumentException ex) {
                help();
            }
        } else {
            outputFilePath = "output/test.txt";
            evaluationType = EvalPT.EvaluationType.ITERATIVE;
        }
        if (cmd.hasOption("i")) {
            inputFilePath = cmd.getOptionValue("i");
        }
        if (cmd.hasOption("o")) {
            outputFilePath = cmd.getOptionValue("o");
        }
        parser.parse(inputFilePath);
        printTime("XML");

        EvalPT pt = handler.getEvalPT();
        MaxMappingSet mappings = new MaxMappingSet();
        PrintStream outStream = new PrintStream(outputFilePath);
        MappingSet set = pt.evaluate(evaluationType);
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

    private void printTime(String event) {
        long estimatedTime = System.nanoTime() - startTime;
        double seconds = (double) estimatedTime / 1000000000.0;
        logger.info("TIME - " + event + ": " + seconds + " sec");
    }

    private void help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("PTEvaluator", options);
        System.exit(0);
    }


}
