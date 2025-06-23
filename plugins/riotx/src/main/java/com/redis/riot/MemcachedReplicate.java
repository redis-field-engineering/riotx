package com.redis.riot;

import com.redis.batch.KeyOperation;
import com.redis.batch.KeyType;
import com.redis.batch.KeyValueEvent;
import com.redis.batch.operation.KeyValueWrite;
import com.redis.riot.core.InetSocketAddressList;
import com.redis.riot.core.MemcachedContext;
import com.redis.riot.core.RedisMemcachedContext;
import com.redis.riot.core.job.RiotStep;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.memcached.MemcachedEntry;
import com.redis.spring.batch.memcached.MemcachedItemReader;
import com.redis.spring.batch.memcached.MemcachedItemWriter;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.springframework.batch.core.Job;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.Collections;

@Command(name = "memcached-replicate", description = "Replicate a Memcached database into another Memcached database.")
public class MemcachedReplicate extends AbstractJobCommand {

    public enum ServerType {
        REDIS, MEMCACHED
    }

    private static final String TASK_NAME = "Replicating";

    private static final String STEP_NAME = "memcached-replicate-step";

    @Parameters(arity = "1", index = "0", description = "Source server address(es) int the form host:port.", paramLabel = "SOURCE")
    private InetSocketAddressList sourceAddressList;

    @Option(names = "--source-tls", description = "Establish a secure TLS connection to source server(s).")
    private boolean sourceTls;

    @Option(names = "--source-tls-host", description = "Hostname for source TLS verification.", paramLabel = "<host>")
    private String sourceTlsHostname;

    @Option(names = "--source-insecure", description = "Skip hostname verification for source server.")
    private boolean sourceInsecure;

    @Option(names = "--target-type", defaultValue = "${RIOT_TARGET_TYPE:-MEMCACHED}", description = "Target type: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<type>")
    private ServerType targetType = ServerType.MEMCACHED;

    @Parameters(arity = "1", index = "1", defaultValue = "${RIOT_TARGET}", description = "Target server URI or host:port.", paramLabel = "TARGET")
    private RedisURI targetUri;

    @CommandLine.ArgGroup(exclusive = false)
    private TargetRedisArgs targetArgs = new TargetRedisArgs();

    @Option(names = "--target-tls-host", description = "Hostname for target TLS verification.", paramLabel = "<host>")
    private String targetTlsHostname;

    private MemcachedContext sourceContext;

    private RedisMemcachedContext targetContext;

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return TASK_NAME;
    }

    @Override
    protected void initialize() throws Exception {
        super.initialize();
        sourceContext = sourceContext();
        targetContext = targetContext();
    }

    private RedisMemcachedContext targetContext() throws Exception {
        if (targetType == ServerType.MEMCACHED) {
            InetSocketAddress address = new InetSocketAddress(targetUri.getHost(), targetUri.getPort());
            MemcachedContext context = new MemcachedContext(Collections.singletonList(address));
            context.setTls(targetArgs.isTls());
            context.setSkipTlsHostnameVerification(targetArgs.isInsecure());
            context.setHostnameForTlsVerification(targetTlsHostname);
            return RedisMemcachedContext.of(context);
        }
        return RedisMemcachedContext.of(targetArgs.redisContext(targetUri));
    }

    private MemcachedContext sourceContext() throws GeneralSecurityException {
        MemcachedContext context = new MemcachedContext(sourceAddressList.getAddresses());
        context.setTls(sourceTls);
        context.setSkipTlsHostnameVerification(sourceInsecure);
        context.setHostnameForTlsVerification(sourceTlsHostname);
        return context;
    }

    @Override
    protected Job job() throws Exception {
        return job(step());
    }

    private RiotStep<MemcachedEntry, ?> step() {
        MemcachedItemReader reader = new MemcachedItemReader(sourceContext::safeMemcachedClient);
        if (targetType == ServerType.MEMCACHED) {
            MemcachedItemWriter writer = new MemcachedItemWriter(targetContext.getMemcachedContext()::safeMemcachedClient);
            return step(STEP_NAME, reader, writer);
        }
        RedisItemWriter<String, byte[], KeyValueEvent<String>> writer = new RedisItemWriter<>(
                RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE), new KeyValueWrite<>());
        RiotStep<MemcachedEntry, KeyValueEvent<String>> step = step(STEP_NAME, reader, writer);
        step.setItemProcessor(this::keyValueEvent);
        return step;
    }

    private KeyValueEvent<String> keyValueEvent(MemcachedEntry entry) {
        KeyValueEvent<String> kv = new KeyValueEvent<>();
        kv.setTimestamp(entry.getTimestamp());
        kv.setKey(entry.getKey());
        kv.setOperation(KeyOperation.READ);
        kv.setType(KeyType.STRING.getString());
        kv.setValue(entry.getValue());
        kv.setTtl(entry.getExpiration());
        return kv;
    }

    public InetSocketAddressList getSourceAddressList() {
        return sourceAddressList;
    }

    public void setSourceAddressList(InetSocketAddressList sourceAddressList) {
        this.sourceAddressList = sourceAddressList;
    }

    public boolean isSourceTls() {
        return sourceTls;
    }

    public void setSourceTls(boolean sourceTls) {
        this.sourceTls = sourceTls;
    }

    public String getSourceTlsHostname() {
        return sourceTlsHostname;
    }

    public void setSourceTlsHostname(String sourceTlsHostname) {
        this.sourceTlsHostname = sourceTlsHostname;
    }

    public boolean isSourceInsecure() {
        return sourceInsecure;
    }

    public void setSourceInsecure(boolean sourceInsecure) {
        this.sourceInsecure = sourceInsecure;
    }

    public String getTargetTlsHostname() {
        return targetTlsHostname;
    }

    public void setTargetTlsHostname(String targetTlsHostname) {
        this.targetTlsHostname = targetTlsHostname;
    }

    public ServerType getTargetType() {
        return targetType;
    }

    public void setTargetType(ServerType targetType) {
        this.targetType = targetType;
    }

    public RedisURI getTargetUri() {
        return targetUri;
    }

    public void setTargetUri(RedisURI targetUri) {
        this.targetUri = targetUri;
    }

    public TargetRedisArgs getTargetArgs() {
        return targetArgs;
    }

    public void setTargetArgs(TargetRedisArgs targetArgs) {
        this.targetArgs = targetArgs;
    }

}
