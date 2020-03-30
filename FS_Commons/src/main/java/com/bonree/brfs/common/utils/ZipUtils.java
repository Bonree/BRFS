package com.bonree.brfs.common.utils;

import com.bonree.brfs.common.supervisor.TimeWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 17:29
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class ZipUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ZipUtils.class);

    public static final String SUFFIX = ".zip";
    private static final int BUFFER_SIZE = 2 * 1024;

    public static void zip(List<String> srcDirs, String outDir) {

        TimeWatcher watcher = new TimeWatcher();
        ZipOutputStream zos = null;

        try {
            OutputStream out = new FileOutputStream(new File(outDir));
            zos = new ZipOutputStream(out);
            List<File> sourceFileList = new ArrayList<File>();
            for (String dir : srcDirs) {
                File sourceFile = new File(dir);
                sourceFileList.add(sourceFile);
            }
            compress(sourceFileList, zos);
            LOG.info("zip complete, srcDirs:{}, outDir:{}, cost time: {}ms", srcDirs, outDir, watcher.getElapsedTime());
        } catch (Exception e) {
            LOG.error("zip err, srcDirs:{}, outDir:{}", srcDirs, outDir, e);
        } finally {
            if (zos != null) {
                try {
                    zos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private static void compress(File sourceFile, ZipOutputStream zos, String name) throws Exception {
        byte[] buf = new byte[BUFFER_SIZE];

        if (sourceFile.isFile()) {
            zos.putNextEntry(new ZipEntry(name));
            int len;
            FileInputStream in = new FileInputStream(sourceFile);
            while ((len = in.read(buf)) != -1) {
                zos.write(buf, 0, len);
            }
            zos.closeEntry();
            in.close();
        } else {
            File[] listFiles = sourceFile.listFiles();
            if (listFiles == null || listFiles.length == 0) {
                zos.putNextEntry(new ZipEntry(name + "/"));
                zos.closeEntry();

            } else {
                for (File file : listFiles) {
                    compress(file, zos, name + "/" + file.getName());
                }
            }
        }
    }

    private static void compress(List<File> sourceFileList, ZipOutputStream zos) throws Exception {
        byte[] buf = new byte[BUFFER_SIZE];

        for (File sourceFile : sourceFileList) {
            String name = sourceFile.getName();
            if (sourceFile.isFile()) {
                zos.putNextEntry(new ZipEntry(name));
                int len;
                FileInputStream in = new FileInputStream(sourceFile);
                while ((len = in.read(buf)) != -1) {
                    zos.write(buf, 0, len);
                }
                zos.closeEntry();
                in.close();
            } else {
                File[] listFiles = sourceFile.listFiles();
                if (listFiles == null || listFiles.length == 0) {
                    zos.putNextEntry(new ZipEntry(name + "/"));
                    zos.closeEntry();
                } else {
                    for (File file : listFiles) {
                        compress(file, zos, name + "/" + file.getName());
                    }
                }
            }
        }
    }

    public static void unZip(String zipPath, String outPutPath) {

        TimeWatcher watcher = new TimeWatcher();

        try {
            ZipInputStream zin = new ZipInputStream(new FileInputStream(zipPath), StandardCharsets.UTF_8);
            BufferedInputStream bin = new BufferedInputStream(zin);
            ZipEntry entry;

            try {
                while ((entry = zin.getNextEntry()) != null && !entry.isDirectory()) {
                    File file = new File(outPutPath, entry.getName());
                    if (!file.exists()) {
                        new File(file.getParent()).mkdirs();
                    }
                    FileOutputStream out = new FileOutputStream(file);
                    BufferedOutputStream Bout = new BufferedOutputStream(out);
                    int b;
                    while ((b = bin.read()) != -1) {
                        Bout.write(b);
                    }
                    Bout.close();
                    out.close();
                }
                bin.close();
                zin.close();
                LOG.info("unzip complete, srcDir:{}, outDir:{}, cost time: {}ms", zipPath, outPutPath, watcher.getElapsedTime());
            } catch (IOException e) {
                LOG.error("unzip err", e);
            }

        } catch (FileNotFoundException e) {
            LOG.error("zip file not exists:{}", zipPath, e);
        }
    }

}
