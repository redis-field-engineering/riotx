package com.redis.riot.file.json;

import com.redis.riot.file.AbstractReaderFactory;
import com.redis.riot.file.ReadOptions;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.core.io.Resource;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonReaderFactory extends AbstractReaderFactory {

	@Override
	public ItemReader<?> create(Resource resource, ReadOptions options) {
		JsonItemReaderBuilder<Object> builder = new JsonItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-json-file-reader");
		builder.resource(resource);
		builder.saveState(false);
		JacksonJsonObjectReader<Object> objectReader = new JacksonJsonObjectReader<>(options.getItemType());
		objectReader.setMapper(objectMapper(new ObjectMapper(), options));
		builder.jsonObjectReader(objectReader);
		if (options.getMaxItemCount() > 0) {
			builder.maxItemCount(options.getMaxItemCount());
		}
		return builder.build();
	}

}
