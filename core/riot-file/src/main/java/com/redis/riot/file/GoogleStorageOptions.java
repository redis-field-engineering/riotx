package com.redis.riot.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.spring.autoconfigure.storage.GcpStorageAutoConfiguration;
import com.google.cloud.spring.core.GcpScope;
import com.google.cloud.spring.core.UserAgentHeaderProvider;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GoogleStorageOptions {

    public static final GcpScope DEFAULT_SCOPE = GcpScope.STORAGE_READ_ONLY;

    private Path keyFile;

    private String projectId;

    private String encodedKey;

    private GcpScope scope = DEFAULT_SCOPE;

    public Storage storage() throws IOException {
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        builder.setProjectId(ServiceOptions.getDefaultProjectId());
        builder.setHeaderProvider(new UserAgentHeaderProvider(GcpStorageAutoConfiguration.class));
        if (keyFile != null) {
            InputStream inputStream;
            inputStream = Files.newInputStream(keyFile);
            builder.setCredentials(credentials(inputStream));
        }
        if (encodedKey != null) {
            byte[] bytes = Base64.getDecoder().decode(encodedKey);
            builder.setCredentials(credentials(new ByteArrayInputStream(bytes)));
        }
        if (projectId != null) {
            builder.setProjectId(projectId);
        }
        return builder.build().getService();
    }

    private GoogleCredentials credentials(InputStream inputStream) throws IOException {
        GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream);
        credentials.createScoped(scope.getUrl());
        return credentials;
    }

    public Path getKeyFile() {
        return keyFile;
    }

    public void setKeyFile(Path keyFile) {
        this.keyFile = keyFile;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getEncodedKey() {
        return encodedKey;
    }

    public void setEncodedKey(String encodedKey) {
        this.encodedKey = encodedKey;
    }

    public GcpScope getScope() {
        return scope;
    }

    public void setScope(GcpScope scope) {
        this.scope = scope;
    }

}
