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

        options.addOption("db", true, "use In-Memory-DB with type eg. 'h2'");
        options.addOption("i", "input", true, "use this xml file as input");
        options.addOption("o", "output", true, "use this file as output");
        options.addOption("h", "help", false, "prints help");
        options.addOption("b", "benchmark", false, "benchmarks all algorithms");
        options.addOption("r", "runs", true, "number of runs");
        options.addOption("ni", "noIndices", false, "does NOT use indices");
    }

    public void parse() throws Exception {
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine cmd = commandLineParser.parse(options, args);
        if (cmd.hasOption("h")) help();


        Boolean useIndices = !cmd.hasOption("ni");
        String inputFilePath = "resources/test.xml";
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
        if (cmd.hasOption("b")) {
            for (int i = 0; i < runs; i++) {
                benchmark.addRun();
                //evaluateAlgorithm(EvalPT.EvaluationType.ITERATIVE, null, inputFilePath, false);
                for (DBConnectionFactory.DBType dbType : DBConnectionFactory.DBType.values()) {
                    evaluateAlgorithm(EvalPT.EvaluationType.DB, dbType, inputFilePath, useIndices);
                }
            }
        } else {
            EvalPT.EvaluationType evaluationType;
            DBConnectionFactory.DBType dbType = null;
            String outputFilePath;

            if (cmd.hasOption("db")) {
                outputFilePath = "output/test-db.txt";
                evaluationType = EvalPT.EvaluationType.DB;
                String db = cmd.getOptionValue("db").toUpperCase();
                try {
                    dbType = DBConnectionFactory.DBType.valueOf(db);
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

    private EvalPT getEvalPT(String inputFilePath) throws SAXException, IOException {
        XMLReader parser = XMLReaderFactory.createXMLReader();
        PTXmlHandler handler = new PTXmlHandler();
        parser.setContentHandler(handler);
        parser.parse(inputFilePath);
        return handler.getEvalPT();
    }

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

    private String modeString(EvalPT.EvaluationType evaluationType, DBConnectionFactory.DBType dbType) {
        String temp = evaluationType.toString();
        if (dbType != null) {
            temp += " - " + dbType.toString();
        }
        return temp;
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
