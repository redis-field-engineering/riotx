package com.redis.riot.db;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class SnowflakeStreamColumnMapRowMapper implements RowMapper<SnowflakeStreamRow> {

    public static final String METADATA_ACTION = "METADATA$ACTION";

    public static final String METADATA_ISUPDATE = "METADATA$ISUPDATE";

    public static final String METADATA_ROW_ID = "METADATA$ROW_ID";

    private RowMapper<Map<String, Object>> delegate = new SnowflakeColumnMapRowMapper();

    public void setDelegate(RowMapper<Map<String, Object>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public SnowflakeStreamRow mapRow(ResultSet rs, int rowNum) throws SQLException {
        Map<String, Object> mapRow = delegate.mapRow(rs, rowNum);
        if (mapRow == null) {
            return null;
        }
        SnowflakeStreamRow row = new SnowflakeStreamRow();
        if (mapRow.containsKey(METADATA_ACTION)) {
            row.setAction(SnowflakeStreamRow.Action.valueOf((String) mapRow.remove(METADATA_ACTION)));
        }
        if (mapRow.containsKey(METADATA_ISUPDATE)) {
            row.setUpdate((Boolean) mapRow.remove(METADATA_ISUPDATE));
        }
        if (mapRow.containsKey(METADATA_ROW_ID)) {
            row.setRowId((String) mapRow.remove(METADATA_ROW_ID));
        }
        row.setColumns(mapRow);
        return row;
    }

}
