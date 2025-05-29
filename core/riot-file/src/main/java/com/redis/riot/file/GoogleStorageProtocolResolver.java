package com.redis.riot.file;

import com.google.cloud.spring.storage.GoogleStorageResource;
import com.google.cloud.storage.Storage;
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

public class GoogleStorageProtocolResolver implements ProtocolResolver {

    private Storage storage;

    @Override
    public Resource resolve(String location, ResourceLoader resourceLoader) {
        if (location.startsWith(com.google.cloud.spring.storage.GoogleStorageProtocolResolver.PROTOCOL)) {
            return new GoogleStorageResource(storage, location, true);
        }
        return null;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

}
