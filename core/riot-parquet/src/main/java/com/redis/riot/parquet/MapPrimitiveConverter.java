package com.redis.riot.parquet;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

class MapPrimitiveConverter extends PrimitiveConverter {

	private final MapGroupConverter parent;
	private final String field;
	private final Type type;

	MapPrimitiveConverter(MapGroupConverter parent, String field, Type type) {
		this.parent = parent;
		this.field = field;
		this.type = type;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.parquet.io.api.PrimitiveConverter#addBinary(Binary)
	 */
	@Override
	public void addBinary(Binary value) {
		if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
			parent.getCurrentRecord().put(field, value.toStringUsingUTF8());
		} else {
			parent.getCurrentRecord().put(field, value.getBytes());
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.parquet.io.api.PrimitiveConverter#addBoolean(boolean)
	 */
	@Override
	public void addBoolean(boolean value) {
		parent.getCurrentRecord().put(field, value);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.parquet.io.api.PrimitiveConverter#addDouble(double)
	 */
	@Override
	public void addDouble(double value) {
		parent.getCurrentRecord().put(field, value);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.parquet.io.api.PrimitiveConverter#addFloat(float)
	 */
	@Override
	public void addFloat(float value) {
		parent.getCurrentRecord().put(field, value);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.parquet.io.api.PrimitiveConverter#addInt(int)
	 */
	@Override
	public void addInt(int value) {
		parent.getCurrentRecord().put(field, value);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.parquet.io.api.PrimitiveConverter#addLong(long)
	 */
	@Override
	public void addLong(long value) {
		parent.getCurrentRecord().put(field, value);
	}

}
