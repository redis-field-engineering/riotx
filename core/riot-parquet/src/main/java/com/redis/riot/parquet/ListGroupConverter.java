package com.redis.riot.parquet;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.io.api.Binary;

class ListGroupConverter extends GroupConverter {

    private final MapGroupConverter parent;
    private final String fieldName;
    private final List<Object> currentList = new ArrayList<>();
    private final Converter repeatedGroupConverter;

    ListGroupConverter(MapGroupConverter parent, String fieldName, GroupType listGroupType) {
        this.parent = parent;
        this.fieldName = fieldName;

        if (listGroupType.getFieldCount() != 1 || !listGroupType.getType(0).isRepetition(Type.Repetition.REPEATED)) {
            throw new IllegalArgumentException("Invalid LIST structure for field: " + fieldName);
        }

        GroupType repeatedGroup = listGroupType.getType(0).asGroupType();
        this.repeatedGroupConverter = new RepeatedGroupConverter(repeatedGroup);
    }

    @Override
    public void start() {
        currentList.clear();
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        if (fieldIndex != 0) throw new IllegalArgumentException("LIST only has one repeated group field");
        return repeatedGroupConverter;
    }

    @Override
    public void end() {
        parent.getCurrentRecord().put(fieldName, currentList.toArray());
    }

    private class RepeatedGroupConverter extends GroupConverter {
        private final Converter elementConverter;

        RepeatedGroupConverter(GroupType repeatedGroup) {
            if (repeatedGroup.getFieldCount() != 1) {
                throw new IllegalArgumentException("Repeated group must have exactly one element field");
            }
            Type elementType = repeatedGroup.getType(0);
            this.elementConverter = new ElementConverter(elementType);
        }

        @Override
        public void start() {
            // nothing
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            if (fieldIndex != 0) throw new IllegalArgumentException("Only element field expected");
            return elementConverter;
        }

        @Override
        public void end() {
            // elementConverter has already added value
        }
    }

    private class ElementConverter extends PrimitiveConverter {
        private final Type elementType;

        ElementConverter(Type elementType) {
            this.elementType = elementType;
        }

        @Override
        public void addFloat(float value) {
            currentList.add(value);
        }

        @Override
        public void addDouble(double value) {
            currentList.add(value);
        }

        @Override
        public void addInt(int value) {
            currentList.add(value);
        }

        @Override
        public void addLong(long value) {
            currentList.add(value);
        }

        @Override
        public void addBoolean(boolean value) {
            currentList.add(value);
        }

        @Override
        public void addBinary(Binary value) {
            if (elementType.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                currentList.add(value.toStringUsingUTF8());
            } else {
                currentList.add(value.getBytes());
            }
        }
    }
}
