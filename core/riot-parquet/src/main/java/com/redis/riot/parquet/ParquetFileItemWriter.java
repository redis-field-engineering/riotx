package com.redis.riot.parquet;

import com.redis.batch.CountingOutputStream;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.ResourceAwareItemWriterItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.item.util.FileUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;

public class ParquetFileItemWriter extends AbstractItemStreamItemWriter<Map<String, Object>>
        implements ResourceAwareItemWriterItemStream<Map<String, Object>>, InitializingBean {

    public static final boolean DEFAULT_ENABLE_DICTIONARY = true;

    public static final int DEFAULT_PAGE_SIZE = 64 * 1024;

    public static final long DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;

    private WritableResource resource;

    private Schema schema;

    private Map<String, String> properties = new LinkedHashMap<>();

    private boolean deleteIfExists = true;

    private CompressionCodecName compression = CompressionCodecName.SNAPPY;

    private long blockSize = DEFAULT_BLOCK_SIZE; // 128MB

    private int pageSize = DEFAULT_PAGE_SIZE; // 64KB

    private boolean enableDictionary = DEFAULT_ENABLE_DICTIONARY;

    private boolean enableValidation;

    private GenericParquetWriter writer;

    /**
     * Setter for a writable resource. Represents a file that can be written.
     *
     * @param resource the resource to be written to
     */
    @Override
    public void setResource(WritableResource resource) {
        this.resource = resource;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public WritableResource getResource() {
        return resource;
    }

    public Schema getSchema() {
        return schema;
    }

    public boolean isDeleteIfExists() {
        return deleteIfExists;
    }

    public void setDeleteIfExists(boolean deleteIfExists) {
        this.deleteIfExists = deleteIfExists;
    }

    public CompressionCodecName getCompression() {
        return compression;
    }

    public void setCompression(CompressionCodecName compression) {
        this.compression = compression;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public boolean isEnableDictionary() {
        return enableDictionary;
    }

    public void setEnableDictionary(boolean enableDictionary) {
        this.enableDictionary = enableDictionary;
    }

    public boolean isEnableValidation() {
        return enableValidation;
    }

    public void setEnableValidation(boolean enableValidation) {
        this.enableValidation = enableValidation;
    }

    /**
     * Flag to indicate that the target file should be deleted if it already exists, otherwise it will be created. Defaults to
     * true.
     *
     * @param shouldDeleteIfExists the flag value to set
     */
    public void setShouldDeleteIfExists(boolean shouldDeleteIfExists) {
        this.deleteIfExists = shouldDeleteIfExists;
    }

    /**
     * Assert that mandatory properties (outputStream and schema) are set.
     *
     * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(resource, "The resource must be set");
        Assert.notNull(schema, "The schema must be set");
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
        if (writer == null) {
            OutputStreamOutputFile outputFile = new OutputStreamOutputFile(outputStream());
            try {
                writer = new GenericParquetWriter(outputFile, schema, configuration(), compression, blockSize, pageSize,
                        enableDictionary, enableValidation);
            } catch (IOException e) {
                throw new ItemStreamException("Could not create writer", e);
            }
        }
    }

    private Configuration configuration() {
        Configuration conf = new Configuration();
        // Configure for modern list structure (compatible with PyArrow)
        conf.setBoolean("parquet.avro.add-list-element-records", false);
        conf.setBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, false);
        if (!CollectionUtils.isEmpty(properties)) {
            properties.forEach(conf::set);
        }
        return conf;
    }

    private OutputStream outputStream() {
        if (resource instanceof FileSystemResource) {
            File file;
            try {
                file = resource.getFile();
            } catch (IOException e) {
                throw new ItemStreamException("Could not convert resource to file: [" + resource + "]", e);
            }
            Assert.state(!file.exists() || file.canWrite(), "Resource is not writable: [" + resource + "]");
            FileUtils.setUpOutputFile(file, false, false, deleteIfExists);
            String path = file.getAbsolutePath();
            try {
                return new FileOutputStream(path, false);
            } catch (FileNotFoundException e) {
                throw new ItemStreamException("Could not create FileOutputStream for file " + path, e);
            }
        }
        try {
            return new CountingOutputStream(resource.getOutputStream());
        } catch (IOException e) {
            throw new ItemStreamException("Could not open output stream", e);
        }
    }

    /**
     * @see ItemStream#update(ExecutionContext)
     */
    @Override
    public synchronized void update(ExecutionContext executionContext) {
        super.update(executionContext);
        if (writer == null) {
            throw new ItemStreamException("Writer not open or already closed.");
        }
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public synchronized void close() throws ItemStreamException {
        super.close();
        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e) {
                throw new ItemStreamException("Could not close writer", e);
            }
        }
    }

    @Override
    public void write(Chunk<? extends Map<String, Object>> chunk) throws Exception {
        for (Map<String, Object> item : chunk) {
            writer.writeRecord(item);
        }
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

}
