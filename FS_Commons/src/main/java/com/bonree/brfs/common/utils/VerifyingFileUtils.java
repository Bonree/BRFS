package com.bonree.brfs.common.utils;

import java.io.File;
import org.slf4j.Logger;

public class VerifyingFileUtils {

    private final boolean warnForRelativePath;
    private final boolean failForNonExistingPath;
    private final Logger log;

    public VerifyingFileUtils(Builder builder) {
        warnForRelativePath = builder.warnForRelativePathOption;
        failForNonExistingPath = builder.failForNonExistingPathOption;
        log = builder.log;
        assert (log != null);
    }

    public File create(String path) {
        File file = new File(path);
        return validate(file);
    }

    public File validate(File file) {
        if (warnForRelativePath) {
            doWarnForRelativePath(file);
        }
        if (failForNonExistingPath) {
            doFailForNonExistingPath(file);
        }
        return file;
    }

    private void doFailForNonExistingPath(File file) {
        if (!file.exists()) {
            throw new IllegalArgumentException(file.toString() + " file is missing");
        }
    }

    private void doWarnForRelativePath(File file) {
        if (file.isAbsolute()) {
            return;
        }
        if (file.getPath().substring(0, 2).equals("." + File.separator)) {
            return;
        }
        log.warn(file.getPath() + " is relative. Prepend ." + File.separator + " to indicate that you're sure!");
    }

    public static class Builder {
        private boolean warnForRelativePathOption = false;
        private boolean failForNonExistingPathOption = false;
        private final Logger log;

        public Builder(Logger log) {
            this.log = log;
        }

        public Builder warnForRelativePath() {
            warnForRelativePathOption = true;
            return this;
        }

        public Builder failForNonExistingPath() {
            failForNonExistingPathOption = true;
            return this;
        }

        public VerifyingFileUtils build() {
            return new VerifyingFileUtils(this);
        }
    }

}
