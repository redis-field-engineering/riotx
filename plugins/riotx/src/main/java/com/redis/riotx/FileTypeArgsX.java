package com.redis.riotx;

import java.util.ArrayList;
import java.util.Map;

import org.springframework.util.MimeType;

import com.redis.riot.FileTypeArgs;

import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;

public class FileTypeArgsX {

	@Option(names = { "-t",
			"--type" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>", completionCandidates = FileTypeCandidates.class, converter = FileTypeConverter.class)
	private MimeType type;

	public MimeType getType() {
		return type;
	}

	public void setType(MimeType type) {
		this.type = type;
	}

	private static final Map<String, MimeType> typeMap = typeMap();

	public static Map<String, MimeType> typeMap() {
		Map<String, MimeType> map = FileTypeArgs.typeMap();
		map.put("parquet", FileImportX.MIME_TYPE_PARQUET);
		return map;
	}

	static class FileTypeConverter implements ITypeConverter<MimeType> {

		@Override
		public MimeType convert(String value) throws Exception {
			return typeMap.get(value.toLowerCase());
		}
	}

	@SuppressWarnings("serial")
	static class FileTypeCandidates extends ArrayList<String> {

		FileTypeCandidates() {
			super(typeMap.keySet());
		}
	}
}
