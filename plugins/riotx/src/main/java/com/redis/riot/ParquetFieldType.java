package com.redis.riot;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public enum ParquetFieldType {

	// Primitive types
	INT32(PrimitiveTypeName.INT32, null), INT64(PrimitiveTypeName.INT64, null), FLOAT(PrimitiveTypeName.FLOAT, null),
	DOUBLE(PrimitiveTypeName.DOUBLE, null), BOOLEAN(PrimitiveTypeName.BOOLEAN, null),

	// String type with logical annotation
	STRING(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()),

	// Byte array types
	BINARY(PrimitiveTypeName.BINARY, null),

	// Date and time types
	DATE(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType()),
	TIMESTAMP(PrimitiveTypeName.INT64,
			LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)),

	// Special types
	UUID(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.uuidType());

	private final PrimitiveTypeName primitiveType;
	private final LogicalTypeAnnotation logicalType;

	ParquetFieldType(PrimitiveTypeName primitiveType, LogicalTypeAnnotation logicalType) {
		this.primitiveType = primitiveType;
		this.logicalType = logicalType;
	}

	public PrimitiveTypeName getPrimitiveType() {
		return primitiveType;
	}

	public LogicalTypeAnnotation getLogicalType() {
		return logicalType;
	}

}