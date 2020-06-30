package com.bonree.brfs.duplication.configuration;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStore {
    private static final Logger log = LoggerFactory.getLogger(LocalStore.class);

    private static final String FILENAME = "local.config";

    private final File storeFile;

    @Inject
    public LocalStore(LocalStoreConfig config) {
        storeFile = new File(config.getStorePath(), FILENAME);
        if (!storeFile.getParentFile().exists()) {
            storeFile.getParentFile().mkdirs();
        }

        if (!storeFile.getParentFile().isDirectory()) {
            throw new IllegalArgumentException(String.format("[%s] is not directory", storeFile.getParentFile()));
        }

        if (storeFile.exists() && !storeFile.isFile()) {
            throw new IllegalArgumentException(String.format("[%s] is not a file", storeFile));
        }

        log.info("local store file path: {}", storeFile.getAbsolutePath());
    }

    public void store(String payload) throws IOException {
        Files.asCharSink(storeFile, StandardCharsets.UTF_8).write(payload);
    }

    public String load() {
        if (!storeFile.isFile()) {
            return null;
        }

        try {
            return Files.asCharSource(storeFile, StandardCharsets.UTF_8).read();
        } catch (IOException e) {
            log.error("read data from [{}] error", storeFile, e);
        }

        return null;
    }
}
