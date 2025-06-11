package com.redis.riot.parquet;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation;

class MapGroupConverter extends GroupConverter {
    protected Map<String, Object> current;
    private final Converter[] converters;

    MapGroupConverter(MapGroupConverter parent, GroupType schema) {
        this.converters = new Converter[schema.getFieldCount()];
        for (int i = 0; i < schema.getFieldCount(); i++) {
            String name = schema.getFieldName(i);
            Type type = schema.getType(i);
            if (type.isPrimitive()) {
                converters[i] = new MapPrimitiveConverter(this, name, type);
            } else {
                GroupType gt = type.asGroupType();
                if (isListType(gt)) {
                    converters[i] = new ListGroupConverter(this, name, gt);
                } else {
                    converters[i] = new MapGroupConverter(this, gt);
                }
            }
        }
    }

    @Override
    public void start() {
        current = new LinkedHashMap<>();
    }

    @Override
    public Converter getConverter(int i) {
        return converters[i];
    }

    @Override
    public void end() { }

    public Map<String, Object> getCurrentRecord() {
        return current;
    }

    private static boolean isListType(GroupType gt) {
        return gt.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
    }
}
