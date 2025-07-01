package com.redis.riot;

import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.ColumnData;
import com.github.freva.asciitable.HorizontalAlign;
import com.redis.batch.KeyType;
import org.springframework.util.unit.DataSize;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StatsPrinter {

    public static final AsciiTableBorder DEFAULT_TABLE_BORDER = AsciiTableBorder.NONE;

    public static final short[] DEFAULT_QUANTILES = { 50, 95, 99 };

    public static final DataSize DEFAULT_WRITE_BANDWIDTH_THRESHOLD = DataSize.ofMegabytes(10);

    private static final String CURSOR_UP = "\033[1A";

    private static final Object CURSOR_DOWN = "\033[K";

    private static final Comparator<? super RedisStats.BigKey> DESC_BANDWIDTH = Comparator.comparing(
            RedisStats.BigKey::writeBandwidth).reversed();

    private static final String NEWLINE = System.lineSeparator();

    private final PrintStream out;

    private final DecimalFormat longFormat = new DecimalFormat("###,###,###");

    private final RedisStats stats;

    private short[] quantiles = DEFAULT_QUANTILES;

    private AsciiTableBorder tableBorder = DEFAULT_TABLE_BORDER;

    private boolean firstWrite = true;

    private int lastDisplayedLines = 0;

    private DataSize writeBandwidthThreshold = DEFAULT_WRITE_BANDWIDTH_THRESHOLD;

    public StatsPrinter(RedisStats stats, PrintStream out) {
        this.stats = stats;
        this.out = out;
    }

    public synchronized void display() {

        // Clear the previous output if not the first write
        if (!firstWrite) {
            // Move cursor up to the beginning of the previous output
            for (int i = 0; i < lastDisplayedLines; i++) {
                out.print(CURSOR_UP); // Move up one line
            }
        } else {
            firstWrite = false;
        }

        Printer printer = new Printer();
        List<RedisStats.Keyspace> keyspaces = stats.keyspaces();
        printer.append(AsciiTable.builder().border(tableBorder.getBorder()).data(keyspaces, statsColumns()).toString());

        printer.append(" ");

        List<RedisStats.BigKey> problemKeys = problemKeys();
        if (problemKeys.isEmpty()) {
            printer.append("No problematic keys detected");
        } else {
            printer.append(
                    AsciiTable.builder().border(tableBorder.getBorder()).data(problemKeys, problemKeyColumns()).toString());
        }

        // Count the number of lines in the output for next refresh
        String outputStr = printer.getString();
        lastDisplayedLines = outputStr.split(NEWLINE).length;

        // Print the output
        out.print(outputStr);
        out.flush();
    }

    private List<RedisStats.BigKey> problemKeys() {
        return stats.bigKeys().stream().filter(this::aboveBandwidth).sorted(DESC_BANDWIDTH).collect(Collectors.toList());
    }

    private boolean aboveBandwidth(RedisStats.BigKey key) {
        return key.writeBandwidth().compareTo(writeBandwidthThreshold) >= 0;
    }

    private static class Printer {

        private final StringBuilder output = new StringBuilder();

        public void append(String multilineString) {
            // Clear each line before printing the table
            String[] lines = multilineString.split(NEWLINE);
            for (String line : lines) {
                output.append(CURSOR_DOWN).append(line).append(NEWLINE);
            }
        }

        public String getString() {
            return output.toString();
        }

    }

    private List<ColumnData<RedisStats.Keyspace>> statsColumns() {
        List<ColumnData<RedisStats.Keyspace>> columns = new ArrayList<>();
        columns.add(string("keyspace", RedisStats.Keyspace::getPrefix));
        columns.add(number("hash", typeCount(KeyType.hash)));
        columns.add(number("json", typeCount(KeyType.json)));
        columns.add(number("list", typeCount(KeyType.list)));
        columns.add(number("set", typeCount(KeyType.set)));
        columns.add(number("stream", typeCount(KeyType.stream)));
        columns.add(number("string", typeCount(KeyType.string)));
        columns.add(number("ts", typeCount(KeyType.timeseries)));
        columns.add(number("zset", typeCount(KeyType.zset)));
        columns.add(number("big", row -> format(row.getBigKeys())));
        for (short quantile : quantiles) {
            columns.add(quantileColumn(quantile));
        }
        return columns;
    }

    private Function<RedisStats.Keyspace, String> typeCount(KeyType type) {
        return row -> format(row.getTypeCounts().getOrDefault(type, 0));
    }

    private ColumnData<RedisStats.Keyspace> quantileColumn(short quantile) {
        return number(String.format("p%s", quantile),
                row -> toString(DataSize.ofBytes(Math.round(row.getMemoryUsage().quantile(quantile / 100)))));
    }

    private List<ColumnData<RedisStats.BigKey>> problemKeyColumns() {
        List<ColumnData<RedisStats.BigKey>> columns = new ArrayList<>();
        columns.add(string("Problem Key", RedisStats.BigKey::getKey));
        columns.add(string("Type", r -> r.getType().name()));
        columns.add(number("Size", k -> toString(k.getMemoryUsage())));
        columns.add(number("Ops/s", k -> format(k.getWriteThroughput())));
        columns.add(number("Rate", k -> toString(k.writeBandwidth())));
        return columns;
    }

    public String toString(DataSize size) {
        if (size.toMegabytes() > 1) {
            return format(size.toMegabytes(), "MB");
        }
        if (size.toKilobytes() > 1) {
            return format(size.toKilobytes(), "KB");
        }
        return format(size.toBytes(), "B");
    }

    private String format(long size, String unit) {
        return String.format("%,d%s", size, unit);
    }

    private String format(long number) {
        return longFormat.format(number);
    }

    private <T> ColumnData<T> string(String header, Function<T, String> getter) {
        return new Column().header(header).headerAlign(HorizontalAlign.RIGHT).dataAlign(HorizontalAlign.LEFT).with(getter);
    }

    private <T> ColumnData<T> number(String header, Function<T, String> getter) {
        return new Column().header(header).headerAlign(HorizontalAlign.RIGHT).dataAlign(HorizontalAlign.RIGHT).with(getter);
    }

    public short[] getQuantiles() {
        return quantiles;
    }

    public void setQuantiles(short[] quantiles) {
        this.quantiles = quantiles;
    }

    public AsciiTableBorder getTableBorder() {
        return tableBorder;
    }

    public void setTableBorder(AsciiTableBorder tableBorder) {
        this.tableBorder = tableBorder;
    }

    public DataSize getWriteBandwidthThreshold() {
        return writeBandwidthThreshold;
    }

    public void setWriteBandwidthThreshold(DataSize writeThroughputThreshold) {
        this.writeBandwidthThreshold = writeThroughputThreshold;
    }

}
