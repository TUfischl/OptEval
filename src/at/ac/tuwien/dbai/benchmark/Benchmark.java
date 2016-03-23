package at.ac.tuwien.dbai.benchmark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;

public class Benchmark {
    static final Logger logger = LogManager.getLogger(Benchmark.class.getName());

    private ArrayList<String> header;
    private ArrayList<String> rowHeader;
    private ArrayList<ArrayList<ArrayList<Double>>> data;
    private long startTime;
    private final int rowWidth = 15;
    private int lineWidth;
    private int timeCount;

    private ArrayList<ArrayList<Double>> currentRun;
    private ArrayList<Double> currentEntry;

    public Benchmark(ArrayList<String> header) {
        this.timeCount = header.size();
        this.header = header;
        this.header.add(0, "Mode");
        this.header.add(1, "Run");
        this.header.add("Sum");
        this.rowHeader = new ArrayList<>();
        this.data = new ArrayList<>();
        this.startTime = System.nanoTime();
        this.lineWidth = (rowWidth + 3) * header.size() + 1;
    }

    public void addRun() {
        currentRun = new ArrayList<>();
        data.add(currentRun);
        logger.info("====================== Run #" + data.size() + " ======================");
    }

    public void addEntry(String mode) {
        rowHeader.add(mode);
        currentEntry = new ArrayList<>();
        currentRun.add(currentEntry);
    }

    public void addTime() {
        long estimatedTime = System.nanoTime() - startTime;
        double seconds = (double) estimatedTime / 1000000000.0;
        currentEntry.add(seconds);
        startTime = System.nanoTime();
    }

    public void print() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(byteArrayOutputStream);
        String formatStringHeader = "| ";
        String formatStringRowHeader = "| %-" + rowWidth + "s | ";
        String formatStringRun = "%-" + rowWidth + "d | ";
        String formatStringRowData = "";
        for (int i = 0; i < header.size(); i++) {
            formatStringHeader += "%-" + rowWidth + "s | ";
        }
        for (int i = 0; i < header.size() - 2; i++) {
            formatStringRowData += "%-" + rowWidth + "f | ";
        }
        formatStringHeader += "\n";
        formatStringRowData += "\n";

        stream.println();
        stream.println(lineSeparator("="));
        stream.format(formatStringHeader, header.toArray());
        stream.println(lineSeparator("="));

        int entryIndex = 0;
        int entryCount = rowHeader.size() / data.size();
        int runCount = data.size();

        while (entryIndex < entryCount) {
            int runIndex = 0;
            Double[] avgRun = new Double[timeCount + 1]; //+1 for sum
            while (runIndex < runCount) {

                ArrayList<ArrayList<Double>> run = data.get(runIndex);
                ArrayList<Double> entry = run.get(entryIndex);
                entry.add(sum(entry));
                if (runIndex == 0) {
                    stream.format(formatStringRowHeader, rowHeader.get(entryIndex));
                } else {
                    stream.format(formatStringRowHeader, "");
                }
                stream.format(formatStringRun, runIndex + 1);
                stream.format(formatStringRowData, entry.toArray());
                if (runIndex == runCount - 1) {
                    stream.println(lineSeparator("="));
                } else {
                    stream.println(lineSeparator());
                }

                //Sum up for AVG
                for (int i = 0; i < avgRun.length; i++) {
                    Double d = avgRun[i];
                    if (d == null) {
                        avgRun[i] = entry.get(i);
                    } else {
                        avgRun[i] += entry.get(i);
                    }
                }

                runIndex++;
            }

            //Print AVG
            for (int i = 0; i < avgRun.length; i++) {
                avgRun[i] /= runCount;
            }
            stream.format("| %" + rowWidth + "s | ", "Avg");
            stream.format("%-" + rowWidth + "s | ", "");
            stream.format(formatStringRowData, avgRun);
            stream.println(lineSeparator("~"));

            entryIndex++;
        }
        logger.info(byteArrayOutputStream.toString());
    }

    private String lineSeparator() {
        return lineSeparator("-");
    }

    private String lineSeparator(String separator) {
        return "+" + repeatString(separator, lineWidth - 2) + "+";
    }

    private String repeatString(String string, int number) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < number; i++) {
            stringBuilder.append(string);
        }
        return stringBuilder.toString();
    }

    private Double sum(ArrayList<Double> list) {
        Double sum = 0d;
        for (Double x : list) {
            sum += x;
        }
        return sum;
    }
}
