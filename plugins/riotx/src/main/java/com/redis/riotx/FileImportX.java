package com.redis.riotx;

import org.springframework.core.io.Resource;
import org.springframework.util.MimeType;

import com.redis.riot.AbstractFileImport;
import com.redis.riot.file.FileReaderRegistry;
import com.redis.riot.file.ReadOptions;
import com.redis.riot.file.RiotResourceMap;
import com.redis.riotx.parquet.ParquetFileItemReader;

import picocli.CommandLine.ArgGroup;

public class FileImportX extends AbstractFileImport {

	public static final MimeType MIME_TYPE_PARQUET = new MimeType("application", "x-parquet");

	@ArgGroup(exclusive = false)
	private FileTypeArgsX fileTypeArgs = new FileTypeArgsX();

	@Override
	protected FileReaderRegistry readerRegistry() {
		FileReaderRegistry registry = super.readerRegistry();
		registry.register(MIME_TYPE_PARQUET, this::parquetFileReader);
		return registry;
	}

	@Override
	protected RiotResourceMap resourceMap() {
		RiotResourceMap resourceMap = super.resourceMap();
		resourceMap.addFileNameMap(new ParquetFileNameMap());
		return resourceMap;
	}

	private ParquetFileItemReader parquetFileReader(Resource resource, ReadOptions options) {
		ParquetFileItemReader reader = new ParquetFileItemReader();
		reader.setResource(resource);
		if (options.getMaxItemCount() > 0) {
			reader.setMaxItemCount(options.getMaxItemCount());
		}
		return reader;
	}

	@Override
	protected MimeType getFileType() {
		return fileTypeArgs.getType();
	}

	public FileTypeArgsX getFileTypeArgs() {
		return fileTypeArgs;
	}

	public void setFileTypeArgs(FileTypeArgsX args) {
		this.fileTypeArgs = args;
	}
}
