package com.redis.riot.parquet;

import org.springframework.util.MimeType;

import java.net.FileNameMap;

public class ParquetFileNameMap implements FileNameMap {

    public static final MimeType MIME_TYPE_PARQUET = new MimeType("application", "x-parquet");

    public static final String PARQUET_SUFFIX = ".parquet";

    @Override
    public String getContentTypeFor(String fileName) {
        if (fileName == null) {
            return null;
        }
        if (fileName.endsWith(PARQUET_SUFFIX)) {
            return MIME_TYPE_PARQUET.toString();
        }
        return null;
    }

}
