package com.redis.riot;

import java.io.File;
import java.time.Duration;

import com.redis.riot.core.ReadFrom;
import com.redis.riot.core.RedisContext;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.protocol.ProtocolVersion;
import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class SingleRedisArgs implements RedisArgs {

    @Option(names = { "-u", "--uri" }, defaultValue = "${RIOT_REDIS_URI}", description = "Redis server URI.", paramLabel = "<uri>")
    private RedisURI uri;

    @Option(names = { "-h",
            "--host" }, defaultValue = "${RIOT_REDIS_HOST:-localhost}", description = "Redis server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
    private String host = DEFAULT_HOST;

    @Option(names = { "-p", "--port" }, defaultValue = "${RIOT_REDIS_PORT:-6379}", description = "Redis server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
    private int port = DEFAULT_PORT;

    @Option(names = { "-s",
            "--socket" }, defaultValue = "${RIOT_REDIS_SOCKET}", description = "Redis server socket (overrides hostname and port).", paramLabel = "<socket>")
    private String socket;

    @Option(names = "--user", defaultValue = "${RIOT_REDIS_USER}", description = "ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
    private String username;

    @Option(names = { "-a",
            "--pass" }, arity = "0..1", interactive = true, defaultValue = "${RIOT_REDIS_PASS}", description = "Password to use when connecting to the Redis server.", paramLabel = "<password>")
    private char[] password;

    @Option(names = "--timeout", defaultValue = "${RIOT_REDIS_TIMEOUT:-60s}", description = "Redis command timeout, e.g. 30s or 5m (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration timeout = DEFAULT_TIMEOUT;

    @Option(names = { "-n", "--db" }, defaultValue = "${RIOT_REDIS_DB:-0}", description = "Redis database number (default: ${DEFAULT-VALUE}).", paramLabel = "<db>")
    private int database = DEFAULT_DATABASE;

    @Option(names = "--tls", defaultValue = "${RIOT_REDIS_TLS}", description = "Establish a secure TLS connection.")
    private boolean tls;

    @Option(names = "--insecure", defaultValue = "${RIOT_REDIS_INSECURE}", description = "Allow insecure TLS connection by skipping cert validation.")
    private boolean insecure;

    @Option(names = "--verify", defaultValue = "${RIOT_REDIS_TLS_VERIFY:-FULL}", description = "TLS verify mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<mode>")
    private SslVerifyMode sslVerifyMode = DEFAULT_SSL_VERIFY_MODE;

    @Option(names = "--client", defaultValue = "${RIOT_REDIS_CLIENT}", description = "Client name used to connect to Redis.", paramLabel = "<name>")
    private String clientName;

    @Option(names = { "-c", "--cluster" }, defaultValue = "${RIOT_REDIS_CLUSTER}", description = "Enable Redis cluster mode.")
    private boolean cluster;

    @Option(names = "--resp", defaultValue = "${RIOT_REDIS_RESP:-RESP2}", description = "Redis protocol version used to connect to Redis: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<ver>")
    private ProtocolVersion protocolVersion = DEFAULT_PROTOCOL_VERSION;

    @Option(names = "--pool", defaultValue = "${RIOT_REDIS_POOL:-8}", description = "Max number of Redis connections (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int poolSize = DEFAULT_POOL_SIZE;

    @Option(names = "--read-from", defaultValue = "${RIOT_REDIS_READ_FROM:-UPSTREAM}", description = "Which Redis cluster nodes to read from: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    private ReadFrom readFrom = DEFAULT_READ_FROM;

    @Option(names = "--keystore", defaultValue = "${RIOT_REDIS_KEYSTORE}", description = "Path to keystore.", paramLabel = "<file>", hidden = true)
    private File keystore;

    @Option(names = "--keystore-pass", arity = "0..1", interactive = true, defaultValue = "${RIOT_REDIS_KEYSTORE_PASS}", description = "Keystore password.", paramLabel = "<password>", hidden = true)
    private char[] keystorePassword;

    @Option(names = "--trust", defaultValue = "${RIOT_REDIS_TRUST}", description = "Path to truststore.", paramLabel = "<file>", hidden = true)
    private File truststore;

    @Option(names = "--trust-pass", arity = "0..1", interactive = true, defaultValue = "${RIOT_REDIS_TRUST_PASS}", description = "Truststore password.", paramLabel = "<password>", hidden = true)
    private char[] truststorePassword;

    @Option(names = "--cert", defaultValue = "${RIOT_REDIS_CERT}", description = "Client certificate to authenticate with (X.509 PEM).", paramLabel = "<file>")
    private File keyCert;

    @Option(names = "--key", defaultValue = "${RIOT_REDIS_KEY}", description = "Private key file to authenticate with (PKCS#8 PEM).", paramLabel = "<file>")
    private File key;

    @Option(names = "--key-pass", arity = "0..1", interactive = true, defaultValue = "${RIOT_REDIS_KEY_PASS}", description = "Private key password.", paramLabel = "<pwd>")
    private char[] keyPassword;

    @Option(names = "--cacert", defaultValue = "${RIOT_REDIS_CACERT}", description = "CA Certificate file to verify with (X.509).", paramLabel = "<file>")
    private File trustedCerts;

    public RedisContext redisContext() {
        return redisContext(redisURI());
    }

    public RedisURI redisURI() {
        return redisURI(uri);
    }

    public RedisURI getUri() {
        return uri;
    }

    public void setUri(RedisURI uri) {
        this.uri = uri;
    }

    @Override
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String getSocket() {
        return socket;
    }

    public void setSocket(String socket) {
        this.socket = socket;
    }

    @Override
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public char[] getPassword() {
        return password;
    }

    public void setPassword(char[] password) {
        this.password = password;
    }

    @Override
    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    @Override
    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    @Override
    public boolean isTls() {
        return tls;
    }

    public void setTls(boolean tls) {
        this.tls = tls;
    }

    @Override
    public boolean isInsecure() {
        return insecure;
    }

    public void setInsecure(boolean insecure) {
        this.insecure = insecure;
    }

    @Override
    public String getClientName() {
        return clientName;
    }

    public void setClientName(String name) {
        this.clientName = name;
    }

    @Override
    public SslVerifyMode getSslVerifyMode() {
        return sslVerifyMode;
    }

    public SingleRedisArgs setSslVerifyMode(SslVerifyMode mode) {
        this.sslVerifyMode = mode;
        return this;
    }

    @Override
    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int size) {
        this.poolSize = size;
    }

    @Override
    public ReadFrom getReadFrom() {
        return readFrom;
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    @Override
    public File getKeystore() {
        return keystore;
    }

    public void setKeystore(File keystore) {
        this.keystore = keystore;
    }

    @Override
    public char[] getKeystorePassword() {
        return keystorePassword;
    }

    public void setKeystorePassword(char[] keystorePassword) {
        this.keystorePassword = keystorePassword;
    }

    @Override
    public File getTruststore() {
        return truststore;
    }

    public void setTruststore(File truststore) {
        this.truststore = truststore;
    }

    @Override
    public char[] getTruststorePassword() {
        return truststorePassword;
    }

    public void setTruststorePassword(char[] truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    @Override
    public File getKeyCert() {
        return keyCert;
    }

    public void setKeyCert(File keyCert) {
        this.keyCert = keyCert;
    }

    @Override
    public File getKey() {
        return key;
    }

    public void setKey(File key) {
        this.key = key;
    }

    @Override
    public char[] getKeyPassword() {
        return keyPassword;
    }

    public void setKeyPassword(char[] keyPassword) {
        this.keyPassword = keyPassword;
    }

    @Override
    public File getTrustedCerts() {
        return trustedCerts;
    }

    public void setTrustedCerts(File trustedCerts) {
        this.trustedCerts = trustedCerts;
    }

    @Override
    public boolean isCluster() {
        return cluster;
    }

    public void setCluster(boolean cluster) {
        this.cluster = cluster;
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(ProtocolVersion version) {
        this.protocolVersion = version;
    }

}
