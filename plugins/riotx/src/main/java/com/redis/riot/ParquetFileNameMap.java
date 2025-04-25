package com.redis.riot;

import java.net.FileNameMap;

public class ParquetFileNameMap implements FileNameMap {

	public static final String PARQUET_SUFFIX = ".parquet";

	@Override
	public String getContentTypeFor(String fileName) {
		if (fileName == null) {
			return null;
		}
		if (fileName.endsWith(PARQUET_SUFFIX)) {
			return FileImport.MIME_TYPE_PARQUET.toString();
		}
		return null;
	}

}
