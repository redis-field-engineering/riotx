package com.redis.riot.parquet;

import java.util.Map;

import org.apache.parquet.hadoop.ParquetReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

public class ParquetFileItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>>
		implements ResourceAwareItemReaderItemStream<Map<String, Object>> {

	private Resource resource;
	private ParquetReader<Map<String, Object>> reader;

	public ParquetFileItemReader() {
		setName(ClassUtils.getShortName(ParquetFileItemReader.class));
	}

	@Override
	public void setResource(Resource resource) {
		this.resource = resource;
	}

	@Override
	protected void doOpen() throws Exception {
		Assert.notNull(resource, "Input resource must be set");
		reader = new ParquetReaderBuilder(new InputStreamInputFile(resource.getInputStream())).build();
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		return reader.read();
	}

	@Override
	protected void doClose() throws Exception {
		if (reader != null) {
			reader.close();
		}
	}

}
