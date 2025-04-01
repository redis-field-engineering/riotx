package com.redis.spring.batch.item;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.ClassUtils;

public class ChunkProcessingItemWriter<S, T> extends ItemStreamSupport implements ItemStreamWriter<S> {

	private final ItemProcessor<Chunk<? extends S>, Chunk<T>> processor;
	private final ItemWriter<T> writer;

	public ChunkProcessingItemWriter(ItemProcessor<Chunk<? extends S>, Chunk<T>> processor, ItemWriter<T> writer) {
		setName(ClassUtils.getShortName(getClass()));
		this.processor = processor;
		this.writer = writer;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		if (processor instanceof ItemStream) {
			((ItemStream) processor).open(executionContext);

		}
		if (writer instanceof ItemStream) {
			((ItemStream) writer).open(executionContext);
		}
	}

	@Override
	public void close() throws ItemStreamException {
		if (writer instanceof ItemStream) {
			((ItemStream) writer).close();
		}
		if (processor instanceof ItemStream) {
			((ItemStream) processor).close();
		}
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		if (processor instanceof ItemStream) {
			((ItemStream) processor).update(executionContext);
		}
		if (writer instanceof ItemStream) {
			((ItemStream) writer).update(executionContext);
		}
	}

	@Override
	public void write(Chunk<? extends S> chunk) throws Exception {
		Chunk<T> processedChunk = processor.process(chunk);
		if (processedChunk != null) {
			writer.write(processedChunk);
		}
	}

}