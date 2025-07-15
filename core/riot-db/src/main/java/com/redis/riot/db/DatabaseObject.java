package com.redis.riot.db;

import org.springframework.util.Assert;

public class DatabaseObject {

    private static final String IDENTIFIER_PATTERN = "[a-zA-Z0-9_$]+";

    private String name;

    private String database;

    private String schema;

    public static DatabaseObject ofSchema(String schema) {
        DatabaseObject object = new DatabaseObject();
        object.setSchema(schema);
        return object;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    private static void check(String value, String name) {
        Assert.isTrue(value.matches(IDENTIFIER_PATTERN), String.format("Invalid %s name format", name));
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the fully qualified name as DATABASE.SCHEMA.TABLE
     */
    public String fullName() {
        return String.format("%s.%s.%s", database, schema, name);
    }

    @Override
    public String toString() {
        return fullName();
    }

    public static DatabaseObject parse(String fullName) {
        Assert.hasText(fullName, "Full name must not be empty");
        String[] parts = fullName.split("\\.");
        Assert.isTrue(parts.length == 3, "Must provide object in format: DATABASE.SCHEMA.TABLE, found " + fullName);
        check(parts[0], "database");
        check(parts[1], "schema");
        check(parts[2], "table");
        DatabaseObject table = new DatabaseObject();
        table.setDatabase(parts[0]);
        table.setSchema(parts[1]);
        table.setName(parts[2]);
        return table;
    }

}
