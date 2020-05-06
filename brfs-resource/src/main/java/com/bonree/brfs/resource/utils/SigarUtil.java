package com.bonree.brfs.resource.utils;

import com.bonree.brfs.resource.impl.SigarGather;
import com.google.common.io.ByteStreams;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SigarUtil {
    private static final Logger LOG = LoggerFactory.getLogger(SigarUtil.class);
    public static String path = null;

    static {
        SigarLoader loader = new SigarLoader(Sigar.class);
        try {
            String libName = loader.getLibraryName();
            final URL url = SigarGather.class.getResource("/lib/" + libName);
            if (url != null) {
                final File tmpDir = Files.createTempDirectory("sigar").toFile();
                tmpDir.deleteOnExit();
                final File nativeLibTmpFile = new File(tmpDir, libName);
                nativeLibTmpFile.deleteOnExit();
                copyToFileAndClose(url.openStream(), nativeLibTmpFile);
                path = nativeLibTmpFile.getParentFile().getAbsolutePath();
                LOG.info("path {}", nativeLibTmpFile.getParentFile().getAbsolutePath());
                LibUtils.loadLibraryPath(nativeLibTmpFile.getParentFile().getAbsolutePath());
                loader.load(nativeLibTmpFile.getParent());
            } else {
                LOG.error("load resource [{}] resource path {}happen error ", libName, SigarGather.class.getResource("/"));
            }
        } catch (Exception e) {
            LOG.error("happen error ", e);
            throw new RuntimeException(e);
        }
    }

    public static Sigar getSigar() {
        return new Sigar();
    }

    public static String getPath() {
        return path;
    }

    public static long copyToFileAndClose(InputStream is, File file) throws IOException {
        file.getParentFile().mkdirs();
        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
            final long result = ByteStreams.copy(is, os);
            os.flush();
            return result;
        } finally {
            is.close();
        }
    }

}
