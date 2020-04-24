package com.bonree.brfs.common.utils;

import java.io.Closeable;
import java.io.IOException;

public final class CloseUtils {

    public static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }

        try {
            closeable.close();
        } catch (IOException e) {
        }
    }

    private CloseUtils() {
    }
}
