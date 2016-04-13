package at.ac.tuwien.dbai.opteval;

import at.ac.tuwien.dbai.benchmark.Benchmark;
import at.ac.tuwien.dbai.db.DBConnectionFactory;
import at.ac.tuwien.dbai.sparql.query.EvalPT;
import at.ac.tuwien.dbai.sparql.query.Mapping;
import at.ac.tuwien.dbai.sparql.query.MappingSet;
import at.ac.tuwien.dbai.sparql.query.MaxMappingSet;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class PTEvaluator {
    static final Logger logger = LogManager.getLogger(PTEvaluator.class.getName());
    static Benchmark benchmark;
    private String[] args = null;
    private Options options = new Options();


    public static void main(String[] args) throws Exception {
        new PTEvaluator(args).parse();
    }

    public PTEvaluator(String[] args) {
        this.args = args;

        options.addOption("db", "database", true, "use In-Memory-DB with type eg. 'h2'");
        options.addOption("i", "input", true, "use this xml file as input");
        options.addOption("o", "output", true, "use this file as output");
        options.addOption("h", "help", false, "prints help");
        options.addOption("b", "benchmark", false, "benchmarks all DB algorithms");
        options.addOption("r", "runs", true, "number of runs");
        options.addOption("ni", "noIndices", false, "does NOT use indices");
    }

    /**
     * The main method ob PTEvaluator, command line arguments are parsed here
     * @throws Exception
     */
    public void parse() throws Exception {
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine cmd = commandLineParser.parse(options, args);
        if (cmd.hasOption("h")) help();


        Boolean useIndices = !cmd.hasOption("ni");
        String inputFilePath = "resources/test.xml"; //Default input file path
        if (cmd.hasOption("i")) {
            inputFilePath = cmd.getOptionValue("i");
        }
        int runs = 1;
        if (cmd.hasOption("r")) {
            String runsString = cmd.getOptionValue("r");
            try {
                runs = Integer.parseInt(runsString);
                if (runs <= 0) help();
            } catch (NumberFormatException ex) {
                help();
            }
        }

        benchmark = new Benchmark(new ArrayList<>(Arrays.asList("Read", "Evaluation", "MaxSet")));
        if (cmd.hasOption("b")) { //Benchmark
            for (int i = 0; i < runs; i++) {
                benchmark.addRun();
                //evaluateAlgorithm(EvalPT.EvaluationType.ITERATIVE, null, inputFilePath, false);
                for (DBConnectionFactory.DBType dbType : DBConnectionFactory.DBType.values()) {
                    evaluateAlgorithm(EvalPT.EvaluationType.DB, dbType, inputFilePath, useIndices);
                }
            }
        } else { //Single algorithm
            EvalPT.EvaluationType evaluationType;
            DBConnectionFactory.DBType dbType = null;
            String outputFilePath;

            if (cmd.hasOption("db")) { //DB
                outputFilePath = "output/test-db.txt"; //Default output file path for db algorithm
                evaluationType = EvalPT.EvaluationType.DB;
                String db = cmd.getOptionValue("db").toUpperCase();
                try {
                    dbType = DBConnectionFactory.DBType.valueOf(db);
                } catch (IllegalArgumentException ex) {
                    help();
                }
            } else { //ITERATIVE
                outputFilePath = "output/test.txt"; //Default output file path for iterative algorithm
                evaluationType = EvalPT.EvaluationType.ITERATIVE;
            }

            if (cmd.hasOption("o")) {
                outputFilePath = cmd.getOptionValue("o");
            }

            MaxMappingSet mappings = null;
            for (int i = 0; i < runs; i++) {
                benchmark.addRun();
                mappings = evaluateAlgorithm(evaluationType, dbType, inputFilePath, useIndices);
            }

            PrintStream outStream = new PrintStream(outputFilePath);
            writeToPrintStream(outStream, mappings); //outputs last run
        }
        benchmark.print();
    }

    /**
     * @param inputFilePath file path to XML input file
     * @return the EvalPT Object for the given XML file path
     * @throws SAXException
     * @throws IOException
     */
    private EvalPT getEvalPT(String inputFilePath) throws SAXException, IOException {
        XMLReader parser = XMLReaderFactory.createXMLReader();
        PTXmlHandler handler = new PTXmlHandler();
        parser.setContentHandler(handler);
        parser.parse(inputFilePath);
        return handler.getEvalPT();
    }

    /**
     * Evaluates one xml document with given algorithm and returns a MaxMappingSet
     * @param evaluationType algorithm to use
     * @param dbType DB to use
     * @param inputFilePath path to xml file
     * @param useIndices should use indices
     * @return the MaxMappingSet of the evaluation
     * @throws IOException
     * @throws SAXException
     */
    private MaxMappingSet evaluateAlgorithm(EvalPT.EvaluationType evaluationType, DBConnectionFactory.DBType dbType, String inputFilePath, Boolean useIndices) throws IOException, SAXException {
        benchmark.addEntry(modeString(evaluationType, dbType));

        EvalPT pt = getEvalPT(inputFilePath);
        benchmark.addTime();

        MappingSet set = pt.evaluate(evaluationType, dbType, useIndices);
        benchmark.addTime();

        MaxMappingSet mappings = new MaxMappingSet();
        mappings.addAll(set);
        benchmark.addTime();

        return mappings;
    }

    /**
     * Helper method to generate a name from an algorithm and/or DB name
     * @param evaluationType
     * @param dbType
     * @return name of algorithm and/or DB name
     */
    private String modeString(EvalPT.EvaluationType evaluationType, DBConnectionFactory.DBType dbType) {
        String temp = evaluationType.toString();
        if (dbType != null) {
            temp += " - " + dbType.toString();
        }
        return temp;
    }

    /**
     * Prints the generated help text and exits application
     */
    private void help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("PTEvaluator", options);
        System.exit(0);
    }

    /**
     * Writes a MaxMapping set to an PrintStream, includes number of rows
     * @param outStream
     * @param mappings
     */
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
