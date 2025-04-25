package com.redis.riot;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.ResourceAwareItemWriterItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.item.util.FileUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;

import com.redis.riot.parquet.OutputStreamOutputFile;
import com.redis.riot.parquet.ParquetWriterBuilder;
import com.redis.spring.batch.resource.CountingOutputStream;

public class ParquetFileItemWriter extends AbstractItemStreamItemWriter<Map<String, Object>>
		implements ResourceAwareItemWriterItemStream<Map<String, Object>>, InitializingBean {

	public static final boolean DEFAULT_TRANSACTIONAL = true;

	protected static final Log logger = LogFactory.getLog(ParquetFileItemWriter.class);

	private WritableResource resource;
	private MessageType schema;

	private boolean deleteIfExists = true;
	private OutputStream outputStream;

	private ParquetWriter<Map<String, Object>> writer;

	/**
	 * Setter for a writable resource. Represents a file that can be written.
	 * 
	 * @param resource the resource to be written to
	 */
	@Override
	public void setResource(WritableResource resource) {
		this.resource = resource;
	}

	public void setSchema(MessageType schema) {
		this.schema = schema;
	}

	/**
	 * Flag to indicate that the target file should be deleted if it already exists,
	 * otherwise it will be created. Defaults to true.
	 * 
	 * @param shouldDeleteIfExists the flag value to set
	 */
	public void setShouldDeleteIfExists(boolean shouldDeleteIfExists) {
		this.deleteIfExists = shouldDeleteIfExists;
	}

	/**
	 * Assert that mandatory properties (outputStream and schema) are set.
	 *
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(resource, "The resource must be set");
		Assert.notNull(schema, "The schema must be set");
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		if (outputStream == null) {
			outputStream = outputStream();
			OutputStreamOutputFile outputFile = new OutputStreamOutputFile(outputStream);
			try {
				writer = new ParquetWriterBuilder(outputFile, schema).build();
			} catch (IOException e) {
				throw new ItemStreamException("Could not create writer", e);
			}
		}
	}

	private OutputStream outputStream() {
		if (resource instanceof FileSystemResource) {
			File file;
			try {
				file = resource.getFile();
			} catch (IOException e) {
				throw new ItemStreamException("Could not convert resource to file: [" + resource + "]", e);
			}
			Assert.state(!file.exists() || file.canWrite(), "Resource is not writable: [" + resource + "]");
			FileUtils.setUpOutputFile(file, false, false, deleteIfExists);
			String path = file.getAbsolutePath();
			try {
				return new FileOutputStream(path, false);
			} catch (FileNotFoundException e) {
				throw new ItemStreamException("Could not create FileOutputStream for file " + path, e);
			}
		}
		try {
			return new CountingOutputStream(resource.getOutputStream());
		} catch (IOException e) {
			throw new ItemStreamException("Could not open output stream", e);
		}
	}

	/**
	 * @see ItemStream#update(ExecutionContext)
	 */
	@Override
	public synchronized void update(ExecutionContext executionContext) {
		super.update(executionContext);
		if (outputStream == null) {
			throw new ItemStreamException("ItemStream not open or already closed.");
		}
		Assert.notNull(executionContext, "ExecutionContext must not be null");
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		super.close();
		if (writer != null) {
			try {
				writer.close();
			} catch (IOException e) {
				throw new ItemStreamException("Could not close writer", e);
			}
			outputStream = null;
		}
	}

	@Override
	public void write(Chunk<? extends Map<String, Object>> chunk) throws Exception {
		for (Map<String, Object> item : chunk) {
			writer.write(item);
		}
		;
	}

}
