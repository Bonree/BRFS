package com.bonree.brfs.common.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {

    private final static Logger LOG = LoggerFactory.getLogger(FileUtils.class);

//    public final static String FILE_SEPARATOR = File.separator;
    public final static String FILE_SEPARATOR = "/";

    /** 概述：创建目录
     * @param pathName 需要创建的目录名
     * @param isRecursion 是否递归创建
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public static void createDir(String pathName, boolean isRecursion) {
        File file = new File(pathName);
        if (isRecursion) {
            if (!file.getParentFile().exists()) {
                createDir(file.getParent(), true);
            }
            file.mkdirs();
        } else {
            file.mkdirs();
        }
    }

    public static boolean isDirectory(String fileName) {
        File file = new File(fileName);
        return file.isDirectory();
    }

    public static boolean isExist(String fileName) {
        File file = new File(fileName);
        return file.exists();
    }

    public static boolean createFile(String fileName, boolean isRecursion) {
        File file = new File(fileName);
        if (isRecursion) {
            if (!file.getParentFile().exists()) {
                createDir(file.getParent(), isRecursion);
            }
            try {
                return file.createNewFile();
            } catch (IOException e) {
                LOG.error("create file " + fileName + " fail!!", e);
                return false;
            }
        } else {
            try {
                return file.createNewFile();
            } catch (IOException e) {
                LOG.error("create file " + fileName + " fail!!", e);
                return false;
            }
        }
    }

    public static List<String> readFileByLine(String fileName) {
        File file = new File(fileName);
        if (file.isDirectory()) {
            throw new IllegalArgumentException("fileName not is directory");
        }
        List<String> lines = new ArrayList<String>(128);
        InputStreamReader reader = null;
        BufferedReader br = null;
        String line = null;
        try {
            reader = new InputStreamReader(new FileInputStream(file));
            br = new BufferedReader(reader);
            while ((line = br.readLine()) != null) {
                if (StringUtils.isNotEmpty(line)) {
                    lines.add(line);
                }
            }
        } catch (FileNotFoundException e) {
            LOG.error("read file error!", e);
        } catch (IOException e) {
            LOG.error("read file error!", e);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOG.error("close BufferedReader error!", e);
                }
            }
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOG.error("close InputStreamReader error!", e);
                }
            }
        }
        return lines;
    }

    public static void writeFileFromList(String fileName, List<String> contents) {
        File file = new File(fileName);
        if (file.isDirectory()) {
            throw new IllegalArgumentException("fileName not is directory");
        }
        OutputStreamWriter writer = null;
        BufferedWriter bw = null;
        try {
            writer = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
            bw = new BufferedWriter(writer);
            for (String record : contents) {
                bw.write(record + "\n");
            }
            bw.flush();
        } catch (UnsupportedEncodingException e) {
            LOG.error("not supported Encoding:", e);
        } catch (FileNotFoundException e) {
            LOG.error("file not found:", e);
        } catch (IOException e) {
            LOG.error("write error:", e);
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    LOG.error("close bw error:", e);
                }
            }
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    LOG.error("close writer error:", e);
                }
            }
        }
    }

    public static List<String> listFileNames(String dir) {
        File file = new File(dir);
        if(file.list() == null|| file.list().length == 0){
        	return null;
        }
        return Arrays.stream(file.list()).collect(Collectors.toList());
    }

    public static List<String> listFileNames(String dir,final String filterEndStr) {
        FilenameFilter filter = new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                if (name.toLowerCase().endsWith(filterEndStr)) {
                    return false;
                }
                return true;
            }
        };

        File file = new File(dir);
        return Arrays.stream(file.list(filter)).collect(Collectors.toList());
    }

    public static List<String> listFilePaths(String dir) {
        File file = new File(dir);
        return Arrays.stream(file.listFiles()).map(File::getPath).collect(Collectors.toList());
    }

    public static List<File> listFiles(String dir) {
        File file = new File(dir);
        return Arrays.stream(file.listFiles()).collect(Collectors.toList());
    }

    /**
     * 概述：删除文件
     * @param filePath
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static boolean deleteFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            return true;
        }
        // 资源回收，强制删除
        
        System.gc();
        return file.delete();
    }
    /**
     * 概述：删除目录
     * @param dirpath
     * @param recursive
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static boolean deleteDir(String dirpath, boolean recursive) {
    	File dir = new File(dirpath);
		Queue<File> fileQueue = new LinkedList<File>();
		LinkedList<File> deletingDirs = new LinkedList<File>();
		fileQueue.add(dir);
		if(!dir.exists()){
			return true;
		}
		
		if(dir.list().length == 0) {
			return dir.delete();
		}
		
		if(!recursive) {
			throw new IllegalStateException("Directory[" + dir.getAbsolutePath() + "] is not empty!");
		}
		boolean isSuccess = true;
		//第一轮先删除普通文件节点
		while(!fileQueue.isEmpty()) {
			File file = fileQueue.poll();
			if(file.isDirectory()) {
				for(File child : file.listFiles()) {
					fileQueue.add(child);
				}
				
				deletingDirs.addFirst(file);
			} else {
				if(!file.delete()){
					isSuccess = false;
				}
			}
		}
		
		//第二轮删除文件夹节点
		for(File deleteDir : deletingDirs) {
			LOG.info("DISK Deleting dir[{}]", deleteDir.getAbsolutePath());
			if(!deleteDir.delete()){
				isSuccess = false;
			}
		}
		return isSuccess;
	}
}
