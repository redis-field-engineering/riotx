package com.redis.riot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.redis.batch.KeyValue;
import com.redis.batch.KeyValueSerializer;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.job.RiotStep;
import com.redis.riot.file.*;
import com.redis.riot.parquet.GenericParquetWriter;
import com.redis.riot.parquet.ParquetFileItemWriter;
import com.redis.riot.parquet.ParquetFileNameMap;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@Command(name = "file-export", description = "Export Redis data to files.")
public class FileExport extends AbstractRedisExport {

    private static final String STEP_NAME = "file-export-step";

    public enum ContentType {
        STRUCT, MAP
    }

    @Parameters(arity = "0..1", description = "File path or URL. If omitted, export is written to stdout.", paramLabel = "FILE")
    private String file = StdOutProtocolResolver.DEFAULT_FILENAME;

    @ArgGroup(exclusive = false)
    private FileWriterArgs fileWriterArgs = new FileWriterArgs();

    @Option(names = "--content-type", description = "Type of exported content: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
    private ContentType contentType = ContentType.STRUCT;

    @ArgGroup(exclusive = false)
    private FileTypeArgs fileTypeArgs = new FileTypeArgs();

    @Option(arity = "1..*", names = "--parquet-field", description = "Parquet fields in the form field1=TYPE. Supported types: INT32, INT64, FLOAT, DOUBLE, BOOLEAN, STRING, BINARY, DATE, TIMESTAMP, UUID", paramLabel = "<f>")
    private Map<String, GenericParquetWriter.FieldType> parquetFields = new LinkedHashMap<>();

    private FileWriterRegistry writerRegistry;

    private ResourceFactory resourceFactory;

    private ResourceMap resourceMap;

    private WriteOptions writeOptions;

    @Override
    protected void initialize() throws Exception {
        super.initialize();
        writerRegistry = writerRegistry();
        resourceFactory = resourceFactory();
        resourceMap = resourceMap();
        writeOptions = writeOptions();
    }

    protected ResourceFactory resourceFactory() {
        ResourceFactory factory = new ResourceFactory();
        factory.addProtocolResolver(new StdOutProtocolResolver());
        return factory;
    }

    private WriteOptions writeOptions() {
        WriteOptions options = fileWriterArgs.fileWriterOptions();
        options.setContentType(getFileType());
        options.setHeaderSupplier(this::headerRecord);
        options.addObjectMapperConfigurer(this::configure);
        return options;
    }

    private void configure(ObjectMapper mapper) {
        SimpleModule module = new SimpleModule();
        module.addSerializer(KeyValue.class, new KeyValueSerializer());
        mapper.registerModule(module);
        mapper.registerModule(new JavaTimeModule());
    }

    @Override
    protected Job job() throws Exception {
        WritableResource resource = resourceFactory.writableResource(file, writeOptions);
        MimeType type = mimeType(resource);
        RiotStep<KeyValue<String>, ?> step = step(STEP_NAME, reader(), writer(resource, type));
        step.setItemProcessor(RiotUtils.processor(keyValueFilter(), processor(type)));
        return job(step);
    }

    private ItemWriter<?> writer(WritableResource resource, MimeType type) {
        WriterFactory writerFactory = writerRegistry.getWriterFactory(type);
        Assert.notNull(writerFactory, String.format("No writer found for file %s", file));
        return writerFactory.create(resource, writeOptions);
    }

    private MimeType mimeType(WritableResource resource) {
        if (writeOptions.getContentType() == null) {
            return resourceMap.getContentTypeFor(resource);
        }
        return writeOptions.getContentType();
    }

    private RedisScanItemReader<String, String> reader() {
        RedisScanItemReader<String, String> reader = RedisScanItemReader.struct();
        configureSource(reader);
        return reader;
    }

    @Override
    protected boolean shouldShowProgress() {
        return super.shouldShowProgress() && file != null;
    }

    protected boolean isFlatFile(MimeType type) {
        return ResourceMap.CSV.equals(type) || ResourceMap.PSV.equals(type) || ResourceMap.TSV.equals(type)
                || ResourceMap.TEXT.equals(type) || ParquetFileNameMap.MIME_TYPE_PARQUET.equals(type);
    }

    private ItemProcessor<KeyValue<String>, ?> processor(MimeType type) {
        if (isFlatFile(type) || contentType == ContentType.MAP) {
            return mapProcessor();
        }
        return null;
    }

    private Map<String, Object> headerRecord() {
        RedisScanItemReader<String, String> reader = RedisScanItemReader.struct();
        configureSource(reader);
        try {
            reader.open(new ExecutionContext());
            try {
                KeyValue<String> keyValue = reader.read();
                if (keyValue == null) {
                    return Collections.emptyMap();
                }
                return mapProcessor().process(keyValue);
            } catch (Exception e) {
                throw new ItemStreamException("Could not read header record", e);
            }
        } finally {
            reader.close();
        }
    }

    protected FileWriterRegistry writerRegistry() {
        FileWriterRegistry registry = FileWriterRegistry.defaultWriterRegistry();
        registry.register(ParquetFileNameMap.MIME_TYPE_PARQUET, this::parquetFileWriter);
        return registry;
    }

    private ParquetFileItemWriter parquetFileWriter(WritableResource resource, WriteOptions options) {
        ParquetFileItemWriter writer = new ParquetFileItemWriter();
        writer.setResource(resource);
        writer.setProperties(options.getProperties());
        Map<String, GenericParquetWriter.FieldType> fields = new LinkedHashMap<>(parquetFields);
        Map<String, Object> sampleRecord = options.getHeaderSupplier().get();
        if (!CollectionUtils.isEmpty(sampleRecord)) {
            sampleRecord.forEach((k, v) -> fields.putIfAbsent(k, inferParquetFieldType(v)));
        }
        writer.setSchema(GenericParquetWriter.createSchema(resource.getFilename(), fields));
        return writer;
    }

    private GenericParquetWriter.FieldType inferParquetFieldType(Object value) {
        if (value instanceof Integer) {
            return GenericParquetWriter.FieldType.INT;
        }
        if (value instanceof Long) {
            return GenericParquetWriter.FieldType.LONG;
        }
        if (value instanceof Float) {
            return GenericParquetWriter.FieldType.FLOAT;
        }
        if (value instanceof Double) {
            return GenericParquetWriter.FieldType.DOUBLE;
        }
        if (value instanceof Boolean) {
            return GenericParquetWriter.FieldType.BOOLEAN;
        }
        if (value instanceof byte[]) {
            return GenericParquetWriter.FieldType.BINARY;
        }
        return GenericParquetWriter.FieldType.STRING;
    }

    protected RiotResourceMap resourceMap() {
        RiotResourceMap resourceMap = RiotResourceMap.defaultResourceMap();
        resourceMap.addFileNameMap(new ParquetFileNameMap());
        return resourceMap;
    }

    protected MimeType getFileType() {
        return fileTypeArgs.getType();
    }

    public FileTypeArgs getFileTypeArgs() {
        return fileTypeArgs;
    }

    public void setFileTypeArgs(FileTypeArgs fileTypeArgs) {
        this.fileTypeArgs = fileTypeArgs;
    }

    public Map<String, GenericParquetWriter.FieldType> getParquetFields() {
        return parquetFields;
    }

    public void setParquetFields(Map<String, GenericParquetWriter.FieldType> parquetFields) {
        this.parquetFields = parquetFields;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public FileWriterArgs getFileWriterArgs() {
        return fileWriterArgs;
    }

    public void setFileWriterArgs(FileWriterArgs fileWriterArgs) {
        this.fileWriterArgs = fileWriterArgs;
    }

    public ContentType getContentType() {
        return contentType;
    }

    public void setContentType(ContentType contentType) {
        this.contentType = contentType;
    }

    public void setWriterRegistry(FileWriterRegistry registry) {
        this.writerRegistry = registry;
    }

}
