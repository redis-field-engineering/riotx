package com.redis.riot.db;

import java.util.Map;

public class SnowflakeStreamRow {

    public enum Action {
        INSERT, DELETE
    }

    private Action action;

    private boolean update;

    private String rowId;

    private Map<String, Object> columns;

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public boolean isUpdate() {
        return update;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public Map<String, Object> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, Object> columns) {
        this.columns = columns;
    }

}
