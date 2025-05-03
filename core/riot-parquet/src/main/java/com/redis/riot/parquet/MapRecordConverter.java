package com.redis.riot.parquet;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class MapRecordConverter extends RecordMaterializer<Map<String, Object>> {

    private final MapGroupConverter root;

    public MapRecordConverter(MessageType schema) {
        this.root = new MapGroupConverter(null, schema);
    }

    @Override
    public Map<String, Object> getCurrentRecord() {
        return root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }

}
