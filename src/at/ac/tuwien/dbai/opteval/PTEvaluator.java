package at.ac.tuwien.dbai.opteval;

import at.ac.tuwien.dbai.db.DBMetaData;
import at.ac.tuwien.dbai.sparql.query.EvalPT;
import at.ac.tuwien.dbai.sparql.query.Mapping;
import at.ac.tuwien.dbai.sparql.query.MappingSet;
import at.ac.tuwien.dbai.sparql.query.MaxMappingSet;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.PrintStream;
import java.util.Iterator;

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
        options.addOption("b", "benchmark", false, "benchmarks all algorithms");
    }

    public void parse() throws Exception {
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine cmd = commandLineParser.parse(options, args);
        if (cmd.hasOption("h")) help();

        startTime = System.nanoTime();
        XMLReader parser = XMLReaderFactory.createXMLReader();
        PTXmlHandler handler = new PTXmlHandler();
        parser.setContentHandler(handler);

        String inputFilePath = "resources/test.xml";
        if (cmd.hasOption("i")) {
            inputFilePath = cmd.getOptionValue("i");
        }
        parser.parse(inputFilePath);
        EvalPT pt = handler.getEvalPT();
        printTime("XML");

        if (cmd.hasOption("b")) {
            evaluateAlgorithm(EvalPT.EvaluationType.ITERATIVE, null, pt);
            for (DBMetaData.DBType dbType : DBMetaData.DBType.values()) {
                evaluateAlgorithm(EvalPT.EvaluationType.DB, dbType, pt);
            }
        } else {
            EvalPT.EvaluationType evaluationType;
            DBMetaData.DBType dbType = null;
            String outputFilePath;

            if (cmd.hasOption("db")) {
                outputFilePath = "output/test-db.txt";
                evaluationType = EvalPT.EvaluationType.DB;
                String db = cmd.getOptionValue("db").toUpperCase();
                try {
                    dbType = DBMetaData.DBType.valueOf(db);
                } catch (IllegalArgumentException ex) {
                    help();
                }
            } else {
                outputFilePath = "output/test.txt";
                evaluationType = EvalPT.EvaluationType.ITERATIVE;
            }

            if (cmd.hasOption("o")) {
                outputFilePath = cmd.getOptionValue("o");
            }

            MaxMappingSet mappings = evaluateAlgorithm(evaluationType, dbType, pt);

            PrintStream outStream = new PrintStream(outputFilePath);
            writeToPrintStream(outStream, mappings);
            printTime("Output");
        }
    }

    private MaxMappingSet evaluateAlgorithm(EvalPT.EvaluationType evaluationType, DBMetaData.DBType dbType, EvalPT pt) {
        MappingSet set = pt.evaluate(evaluationType, dbType);
        printTime("evaluate");

        MaxMappingSet mappings = new MaxMappingSet();
        mappings.addAll(set);
        printTime("MaxSet");

        return mappings;
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

    private void writeToPrintStream(PrintStream outStream, MaxMappingSet mappings) {
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
    }
}
