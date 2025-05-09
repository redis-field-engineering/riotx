package com.redis.riot.rdi;

/**
 * Descriptor for the structure of Debezium message objects representing CREATE, READ, UPDATE, and DELETE messages that conform
 * to that schema.
 */
public class Message {


    /**
     * The operation that read the current state of a record, most typically during snapshots.
     */
    public static final String OPERATION_READ = "r";

    /**
     * An operation that resulted in a new record being created in the source.
     */
    public static final String OPERATION_CREATE = "c";

    /**
     * An operation that resulted in an existing record being updated in the source.
     */
    public static final String OPERATION_UPDATE = "u";

    /**
     * An operation that resulted in an existing record being removed from or deleted in the source.
     */
    public static final String OPERATION_DELETE = "d";

    /**
     * An operation that resulted in an existing table being truncated in the source.
     */
    public static final String OPERATION_TRUNCATE = "t";

    /**
     * An operation that resulted in a generic message
     */
    public static final String OPERATION_MESSAGE = "m";


    /**
     * The {@code before} field is used to store the state of a record before an operation.
     */
    private Object before;

    /**
     * The {@code after} field is used to store the state of a record after an operation.
     */
    private Object after;

    /**
     * The {@code origin} field is used to store the information about the source of a record, including the Kafka Connect
     * partition and offset information.
     */
    private Source source;

    /**
     * The {@code op} field is used to store the kind of operation on a record.
     */
    private String op;

    private Object transaction;

    /**
     * The {@code ts_ms} field is used to store the information about the local time at which the connector processed/generated
     * the event. The timestamp values are the number of milliseconds past epoch (January 1, 1970), and determined by the
     * {@link System#currentTimeMillis() JVM current time in milliseconds}. Note that the
     * <em>accuracy</em> of the timestamp value depends on the JVM's system clock and all of its assumptions, limitations,
     * conditions, and variations.
     */
    private long ts_ms;

    /**
     * The {@code ts_us} field represents the timestamp but in microseconds.
     */
    private long ts_us;

    /**
     * The {@code ts_ns} field represents the timestamp but in nanoseconds.
     */
    private long ts_ns;

    /**
     * Message.source field
     */
    public static class Source {

        private String version; // version e.g. "2.7.3.Final"

        private String connector; // connector name e.g. "postgresql"

        private String name; // server name e.g. "rdi"

        private long ts_ms; // millis timestamp e.g. 1740785606068

        private long ts_us; // micros timestamp e.g. 1740785606068813

        private long ts_ns; // nanos timestamp  e.g. 1740785606068813000

        private String snapshot; // "first_in_data_collection"

        private String db; // "chinook"

        private String schema; // "public"

        private String table; // "Track";

        private Object sequence;

        private Object txId;

        private Object lsn;

        private Object xmin;

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public String getConnector() {
            return connector;
        }

        public void setConnector(String connector) {
            this.connector = connector;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getTs_ms() {
            return ts_ms;
        }

        public void setTs_ms(long ts_ms) {
            this.ts_ms = ts_ms;
        }

        public long getTs_us() {
            return ts_us;
        }

        public void setTs_us(long ts_us) {
            this.ts_us = ts_us;
        }

        public long getTs_ns() {
            return ts_ns;
        }

        public void setTs_ns(long ts_ns) {
            this.ts_ns = ts_ns;
        }

        public String getSnapshot() {
            return snapshot;
        }

        public void setSnapshot(String snapshot) {
            this.snapshot = snapshot;
        }

        public String getDb() {
            return db;
        }

        public void setDb(String db) {
            this.db = db;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public Object getSequence() {
            return sequence;
        }

        public void setSequence(Object sequence) {
            this.sequence = sequence;
        }

        public Object getTxId() {
            return txId;
        }

        public void setTxId(Object txId) {
            this.txId = txId;
        }

        public Object getLsn() {
            return lsn;
        }

        public void setLsn(Object lsn) {
            this.lsn = lsn;
        }

        public Object getXmin() {
            return xmin;
        }

        public void setXmin(Object xmin) {
            this.xmin = xmin;
        }

    }

    public Object getBefore() {
        return before;
    }

    public void setBefore(Object before) {
        this.before = before;
    }

    public Object getAfter() {
        return after;
    }

    public void setAfter(Object after) {
        this.after = after;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public long getTs_ms() {
        return ts_ms;
    }

    public void setTs_ms(long ts_ms) {
        this.ts_ms = ts_ms;
    }

    public long getTs_us() {
        return ts_us;
    }

    public void setTs_us(long ts_us) {
        this.ts_us = ts_us;
    }

    public long getTs_ns() {
        return ts_ns;
    }

    public void setTs_ns(long ts_ns) {
        this.ts_ns = ts_ns;
    }

    public Object getTransaction() {
        return transaction;
    }

    public void setTransaction(Object transaction) {
        this.transaction = transaction;
    }

}
