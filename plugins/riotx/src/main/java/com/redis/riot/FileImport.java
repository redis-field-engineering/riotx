package com.redis.riot;

import com.redis.riot.core.KeyValueDeserializer;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.function.MapToFieldFunction;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.riot.core.function.ToMapFunction;
import com.redis.riot.core.job.RiotStep;
import com.redis.riot.file.*;
import com.redis.riot.parquet.ParquetFileItemReader;
import com.redis.riot.parquet.ParquetFileNameMap;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Command(name = "file-import", description = "Import data from files.")
public class FileImport extends AbstractRedisImport {

    public static final String STDIN_FILENAME = "-";

    private static final Set<MimeType> keyValueTypes = new HashSet<>(
            Arrays.asList(ResourceMap.JSON, ResourceMap.JSON_LINES, ResourceMap.XML));

    @ArgGroup(exclusive = false)
    private FileTypeArgs fileTypeArgs = new FileTypeArgs();

    @Parameters(arity = "1..*", description = "Files or URLs to import. Use '-' to read from stdin.", paramLabel = "FILE")
    private List<String> files;

    @ArgGroup(exclusive = false)
    private FileReaderArgs fileReaderArgs = new FileReaderArgs();

    @Option(arity = "1..*", names = "--regex", description = "Regular expressions used to extract values from fields in the form field1=\"regex\" field2=\"regex\"...", paramLabel = "<f=rex>")
    private Map<String, Pattern> regexes = new LinkedHashMap<>();

    private FileReaderRegistry readerRegistry;

    private RiotResourceMap resourceMap;

    private ResourceFactory resourceFactory;

    private ReadOptions readOptions;

    private FileReaderRegistry readerRegistry() {
        FileReaderRegistry registry = FileReaderRegistry.defaultReaderRegistry();
        registry.register(ParquetFileNameMap.MIME_TYPE_PARQUET, this::parquetFileReader);
        return registry;
    }

    protected RiotResourceMap resourceMap() {
        RiotResourceMap resourceMap = RiotResourceMap.defaultResourceMap();
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

    private MimeType getFileType() {
        return fileTypeArgs.getType();
    }

    @Override
    protected void initialize() throws Exception {
        super.initialize();
        Assert.notEmpty(files, "No file specified");
        readerRegistry = readerRegistry();
        resourceFactory = resourceFactory();
        resourceMap = resourceMap();
        readOptions = readOptions();
    }

    protected ResourceFactory resourceFactory() {
        ResourceFactory resourceFactory = new ResourceFactory();
        StdInProtocolResolver stdInProtocolResolver = new StdInProtocolResolver();
        stdInProtocolResolver.setFilename(STDIN_FILENAME);
        resourceFactory.addProtocolResolver(stdInProtocolResolver);
        return resourceFactory;
    }

    @Override
    protected Job job() {
        return job(files.stream().map(this::step).collect(Collectors.toList()));
    }

    private RiotStep<?, ?> step(String location) {
        Resource resource;
        try {
            resource = resourceFactory.resource(location, readOptions);
        } catch (IOException e) {
            throw new RuntimeIOException(String.format("Could not create resource from %s", location), e);
        }
        MimeType type = contentType(resource);
        ReaderFactory readerFactory = readerRegistry.getReaderFactory(type);
        Assert.notNull(readerFactory, () -> String.format("No reader found for file %s", location));
        ItemReader<?> reader = readerFactory.create(resource, readOptions);
        RedisItemWriter<?, ?, ?> writer = writer();
        configureTargetRedisWriter(writer);
        RiotStep<?, ?> step = step(resource.getFilename(), reader, writer);
        step.name(location);
        if (hasOperations()) {
            step.processor(RiotUtils.processor(operationProcessor(), regexProcessor()));
        } else {
            Assert.isTrue(keyValueTypes.contains(type), "No Redis operation specified");
        }
        step.skip(ParseException.class);
        step.skip(org.springframework.batch.item.ParseException.class);
        step.noRetry(ParseException.class);
        step.noRetry(org.springframework.batch.item.ParseException.class);
        return step;
    }

    private MimeType contentType(Resource resource) {
        if (readOptions.getContentType() == null) {
            return resourceMap.getContentTypeFor(resource);
        }
        return readOptions.getContentType();
    }

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return String.format("Importing %s", step.getName());
    }

    private ReadOptions readOptions() {
        ReadOptions options = fileReaderArgs.readOptions();
        options.setContentType(getFileType());
        options.setItemType(itemType());
        options.addDeserializer(KeyValue.class, new KeyValueDeserializer());
        return options;
    }

    private Class<?> itemType() {
        if (hasOperations()) {
            return Map.class;
        }
        return KeyValue.class;
    }

    private RedisItemWriter<?, ?, ?> writer() {
        if (hasOperations()) {
            return operationWriter();
        }
        return RedisItemWriter.struct();
    }

    private ItemProcessor<Map<String, Object>, Map<String, Object>> regexProcessor() {
        if (CollectionUtils.isEmpty(regexes)) {
            return null;
        }
        List<Function<Map<String, Object>, Map<String, Object>>> functions = new ArrayList<>();
        functions.add(Function.identity());
        regexes.entrySet().stream().map(e -> toFieldFunction(e.getKey(), e.getValue())).forEach(functions::add);
        return new FunctionItemProcessor<>(new ToMapFunction<>(functions));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Function<Map<String, Object>, Map<String, Object>> toFieldFunction(String key, Pattern regex) {
        return new MapToFieldFunction(key).andThen((Function) new RegexNamedGroupFunction(regex));
    }

    public List<String> getFiles() {
        return files;
    }

    public void setFiles(String... files) {
        setFiles(Arrays.asList(files));
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }

    public FileReaderArgs getFileReaderArgs() {
        return fileReaderArgs;
    }

    public void setFileReaderArgs(FileReaderArgs args) {
        this.fileReaderArgs = args;
    }

    public Map<String, Pattern> getRegexes() {
        return regexes;
    }

    public void setRegexes(Map<String, Pattern> regexes) {
        this.regexes = regexes;
    }

    public FileReaderRegistry getReaderRegistry() {
        return readerRegistry;
    }

    public void setReaderRegistry(FileReaderRegistry registry) {
        this.readerRegistry = registry;
    }

    public FileTypeArgs getFileTypeArgs() {
        return fileTypeArgs;
    }

    public void setFileTypeArgs(FileTypeArgs args) {
        this.fileTypeArgs = args;
    }

}
