package com.redis.riotx;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.MessageTypeBuilder;
import org.apache.parquet.schema.Types.PrimitiveBuilder;
import org.springframework.core.io.WritableResource;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;

import com.redis.riot.AbstractFileExport;
import com.redis.riot.FileTypeArgs;
import com.redis.riot.file.FileWriterRegistry;
import com.redis.riot.file.RiotResourceMap;
import com.redis.riot.file.WriteOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "file-export", description = "Export Redis data to files.")
public class FileExportX extends AbstractFileExport {

	@ArgGroup(exclusive = false)
	private FileTypeArgs fileTypeArgs = new FileTypeArgs();

	@Option(arity = "1..*", names = "--parquet-field", description = "Parquet fields in the form field1=TYPE. Supported types: INT32, INT64, FLOAT, DOUBLE, BOOLEAN, STRING, BINARY, DATE, TIMESTAMP, UUID", paramLabel = "<f>")
	private Map<String, ParquetFieldType> parquetFields = new LinkedHashMap<>();

	@Override
	protected boolean isFlatFile(MimeType type) {
		return super.isFlatFile(type) || FileImportX.MIME_TYPE_PARQUET.equals(type);
	}

	@Override
	protected FileWriterRegistry writerRegistry() {
		FileWriterRegistry registry = super.writerRegistry();
		registry.register(FileImportX.MIME_TYPE_PARQUET, this::parquetFileWriter);
		return registry;
	}

	private ParquetFileItemWriter parquetFileWriter(WritableResource resource, WriteOptions options) {
		ParquetFileItemWriter writer = new ParquetFileItemWriter();
		writer.setResource(resource);
		Map<String, ParquetFieldType> fields = new LinkedHashMap<>(parquetFields);
		Map<String, Object> sampleRecord = options.getHeaderSupplier().get();
		if (!CollectionUtils.isEmpty(sampleRecord)) {
			sampleRecord.forEach((k, v) -> fields.putIfAbsent(k, inferParquetFieldType(v)));
		}
		writer.setSchema(schema(resource.getFilename(), fields));
		return writer;
	}

	private ParquetFieldType inferParquetFieldType(Object value) {
		if (value instanceof Integer) {
			return ParquetFieldType.INT32;
		}
		if (value instanceof Long) {
			return ParquetFieldType.INT64;
		}
		if (value instanceof Float) {
			return ParquetFieldType.FLOAT;
		}
		if (value instanceof Double) {
			return ParquetFieldType.DOUBLE;
		}
		if (value instanceof Boolean) {
			return ParquetFieldType.BOOLEAN;
		}
		if (value instanceof byte[]) {
			return ParquetFieldType.BINARY;
		}
		if (value instanceof Date) {
			return ParquetFieldType.DATE;
		}
		if (value instanceof UUID) {
			return ParquetFieldType.UUID;
		}
		return ParquetFieldType.STRING;
	}

	@Override
	protected RiotResourceMap resourceMap() {
		RiotResourceMap resourceMap = super.resourceMap();
		resourceMap.addFileNameMap(new ParquetFileNameMap());
		return resourceMap;
	}

	@Override
	protected MimeType getFileType() {
		return fileTypeArgs.getType();
	}

	public FileTypeArgs getFileTypeArgs() {
		return fileTypeArgs;
	}

	public void setFileTypeArgs(FileTypeArgs fileTypeArgs) {
		this.fileTypeArgs = fileTypeArgs;
	}

	public Map<String, ParquetFieldType> getParquetFields() {
		return parquetFields;
	}

	public void setParquetFields(Map<String, ParquetFieldType> parquetFields) {
		this.parquetFields = parquetFields;
	}

	public static MessageType schema(String name, Map<String, ParquetFieldType> fieldMap) {
		MessageTypeBuilder schema = Types.buildMessage();
		fieldMap.forEach((k, v) -> {
			PrimitiveBuilder<PrimitiveType> builder = Types.optional(v.getPrimitiveType());
			if (v.getLogicalType() != null) {
				builder = builder.as(v.getLogicalType());
			}
			schema.addField(builder.named(k));
		});
		return schema.named(name);
	}

}
