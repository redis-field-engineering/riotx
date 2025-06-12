package com.redis.riot.parquet;

import com.redis.spring.batch.item.AbstractCountingItemReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.LinkedHashMap;
import java.util.Map;

public class ParquetFileItemReader extends AbstractCountingItemReader<Map<String, Object>>
        implements ResourceAwareItemReaderItemStream<Map<String, Object>> {

    private Resource resource;

    private Map<String, String> properties = new LinkedHashMap<>();

    private GenericParquetReader reader;

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(resource, "Input resource must be set");
        reader = new GenericParquetReader(new InputStreamInputFile(resource.getInputStream()), configuration());
    }

    private Configuration configuration() {
        Configuration conf = new Configuration();
        conf.setBoolean("parquet.avro.add-list-element-records", false);
        conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
        conf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
        if (!CollectionUtils.isEmpty(properties)) {
            properties.forEach(conf::set);
        }
        return conf;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * @param properties Avro configuration to set on the reader. See <a
     * href="https://github.com/apache/parquet-java/blob/master/parquet-avro/README.md">README</a>
     */
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    protected Map<String, Object> doRead() throws Exception {
        if (reader.hasNext()) {
            return reader.nextRecord();
        }
        return null;
    }

    @Override
    protected void doClose() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

}
