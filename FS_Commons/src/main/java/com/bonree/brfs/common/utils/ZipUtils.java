package com.bonree.brfs.common.utils;

import com.bonree.brfs.common.supervisor.TimeWatcher;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            try {
                ZipEntry zipEntry = null;
                byte[] buffer = new byte[BUFFER_SIZE]; //缓冲器
                int readLength = 0; //每次读出来的长度

                while ((zipEntry = zin.getNextEntry()) != null) {
                    if (zipEntry.isDirectory()) {   //若是zip条目目录，则需创建这个目录
                        File dir = new File(outPutPath + "/" + zipEntry.getName());
                        if (!dir.exists()) {
                            dir.mkdirs();
                            continue; //跳出
                        }
                    }

                    File file = createFile(outPutPath, zipEntry.getName()); //若是文件，则需创建该文件
                    OutputStream outputStream = new FileOutputStream(file);

                    while ((readLength = zin.read(buffer, 0, BUFFER_SIZE)) != -1) {
                        outputStream.write(buffer, 0, readLength);
                    }
                    outputStream.close();
                }    // end while
                LOG.info("unzip complete, srcDir:{}, outDir:{}, cost time: {}ms", zipPath, outPutPath, watcher.getElapsedTime());
            } catch (IOException e) {
                LOG.error("unzip err", e);
            }

        } catch (FileNotFoundException e) {
            LOG.error("zip file not exists:{}", zipPath, e);
        }
    }

    private static File createFile(String dstPath, String fileName) {
        String[] dirs = fileName.split("/"); //将文件名的各级目录分解
        File file = new File(dstPath);

        if (dirs.length > 1) { //文件有上级目录
            for (int i = 0; i < dirs.length - 1; i++) {
                file = new File(file, dirs[i]); //依次创建文件对象知道文件的上一级目录
            }
            if (!file.exists()) {
                file.mkdirs(); //文件对应目录若不存在，则创建
            }
            file = new File(file, dirs[dirs.length - 1]); //创建文件
        } else {
            if (!file.exists()) {
                file.mkdirs(); //若目标路径的目录不存在，则创建
            }
            file = new File(file, dirs[0]); //创建文件
        }
        return file;
    }

}
