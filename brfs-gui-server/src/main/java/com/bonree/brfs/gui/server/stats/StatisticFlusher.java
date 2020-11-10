package com.bonree.brfs.gui.server.stats;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticFlusher implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(StatisticFlusher.class);
    private static final String READ_DIR = "read";
    private static final String WRITE_DIR = "write";
    private static final String SPACE_SEPARATOR = " ";
    private String basePath;
    private Collection<String> scanPaths = null;
    private ScheduledExecutorService pool = null;
    private int intervalTime;
    private int ttlTime;
    private boolean run = true;
    private boolean start;

    public StatisticFlusher(String path, int scanIntervalTime, int ttlTime) {
        this.basePath = path;
        this.intervalTime = scanIntervalTime;
        this.ttlTime = ttlTime;

    }

    @Inject
    public StatisticFlusher(StatConfigs configs) {
        this.basePath = configs.getBaseDir();
        this.intervalTime = configs.getScanIntervalTime();
        this.ttlTime = configs.getTtlTime();

    }

    // basePath/read/read-statistic-2020-04-28
    private String createPath(String type) {
        return new StringBuilder(this.basePath)
            .append(File.separator)
            .append(type)
            .append(File.separator)
            .toString();
    }

    /**
     * 加载数据
     *
     * @param dirPath
     * @param time
     * @param clazz
     * @param <T>
     *
     * @return
     */
    private <T> Collection<T> collectObject(String dirPath, long time, Class clazz) {
        File dir = new File(dirPath);
        String timeStamp = formatMin(time);
        Collection<T> results = new ArrayList<>();
        Collection<File> files = collectBelongFile(dir, timeStamp);
        for (File file : files) {
            byte[] data = readFile(file);
            if (data == null || data.length == 0) {
                continue;
            }
            Object obj = JsonUtils.toObjectQuietly(data, clazz);
            if (obj != null) {
                results.add((T) obj);
            }
        }
        return results;
    }

    protected <T> Collection<T> collectObjects(String dirPath, long time, Class clazz) {
        File dir = new File(dirPath);
        String timeStamp = formatMin(time);
        Collection<T> results = new ArrayList<>();
        Collection<File> files = collectBelongFile(dir, timeStamp);
        for (File file : files) {
            byte[] data = readFile(file);
            if (data == null || data.length == 0) {
                continue;
            }
            T[] objs = (T[]) JsonUtils.toObjectQuietly(data, clazz);
            if (objs != null && objs.length != 0) {
                for (T t : objs) {
                    results.add(t);
                }
            }
        }
        return results;
    }

    private void saveObject(String path, byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }
        File file = new File(path);
        try {
            FileUtils.writeByteArrayToFile(file, data, false);
        } catch (IOException e) {
            LOG.error("save happen error content: {}", new String(data), e);
        }
    }

    /**
     * 读取文件
     *
     * @param file
     *
     * @return
     */
    private byte[] readFile(File file) {
        try {
            if (!file.exists()) {
                return null;
            }
            return FileUtils.readFileToByteArray(file);
        } catch (IOException e) {
            LOG.error("read [{}] happen error", file.getName(), e);
        }
        return null;
    }

    private String formatDay(long time) {
        return TimeUtils.formatTimeStamp(time, "yyyyMMdd");
    }

    private String formatMin(long time) {
        return TimeUtils.formatTimeStamp(time, "yyyy-MM-dd HH:mm");
    }

    /**
     * 查找指定时间以前的文件
     *
     * @param dir
     * @param belongTime
     *
     * @return
     */
    private Collection<File> collectBelongFile(File dir, String belongTime) {
        try {
            if (!dir.exists()) {
                return new ArrayList<>();
            }
            return FileUtils.listFiles(dir, new IOFileFilter() {
                @Override
                public boolean accept(File file) {
                    return belongTime.compareTo(file.getName()) <= 0;
                }

                @Override
                public boolean accept(File file, String s) {
                    return true;
                }
            }, new IOFileFilter() {
                @Override
                public boolean accept(File file) {
                    return false;
                }

                @Override
                public boolean accept(File file, String s) {
                    return false;
                }
            });
        } catch (Exception e) {
            LOG.error("query {} happen error !!", dir.getName(), e);
        }
        return new ArrayList<>();
    }

    /**
     * 查找过期文件
     *
     * @param dir
     * @param expectTime
     *
     * @return
     */
    private Collection<File> collectTTLFile(File dir, long expectTime) {
        try {
            if (!dir.exists()) {
                return new ArrayList<>();
            }
            return FileUtils.listFiles(dir, new IOFileFilter() {
                @Override
                public boolean accept(File file) {
                    return expectTime - Long.valueOf(file.getName()) > 0;
                }

                @Override
                public boolean accept(File file, String s) {
                    return true;
                }
            }, new IOFileFilter() {
                @Override
                public boolean accept(File file) {
                    return true;
                }

                @Override
                public boolean accept(File file, String s) {
                    return true;
                }
            });
        } catch (Exception e) {
            LOG.error("scan ttl file {} happen error !!", dir.getName(), e);
        }
        return new ArrayList<>();
    }

    private String createRoot(String type) {
        return new StringBuilder(this.basePath).append(File.separator).append(type).toString();
    }

    @LifecycleStart
    public void start() throws Exception {
        scanPaths = Arrays.asList(
            createRoot(WRITE_DIR),
            createRoot(READ_DIR));
        pool = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder().setNameFormat("statFileFlusher").build());
        start = true;
        pool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                scanPaths.stream().forEach(y -> {
                    if (!start) {
                        LOG.info("stop it ");
                        return;
                    }
                    File rootDir = new File(y);
                    if (!rootDir.exists()) {
                        LOG.info("{} not exists, create it", rootDir.getName());
                        rootDir.mkdirs();
                    }
                    if (!rootDir.isDirectory()) {
                        LOG.info("{} not dir delete it ", rootDir.getName());
                        return;
                    }
                    long time = System.currentTimeMillis() - ttlTime;
                    File[] childs = rootDir.listFiles();
                    if (childs == null || childs.length == 0) {
                        return;
                    }
                    Collection<File> sumLoss = new ArrayList<>();
                    for (File file : childs) {
                        Collection<File> loss = StatisticFlusher.this.collectTTLFile(file, time);
                        if (!loss.isEmpty()) {
                            sumLoss.addAll(loss);
                        }
                    }
                    if (!sumLoss.isEmpty() && start) {
                        sumLoss.forEach(FileUtils::deleteQuietly);
                    }
                    LOG.info("{} delete {} file", rootDir.getName(), sumLoss.size());
                });
            }
        }, 0, intervalTime, TimeUnit.SECONDS);
    }

    @Override
    public void stop() throws Exception {
        start = false;
        if (pool != null) {
            pool.shutdown();
        }
    }

    public void flush(long currentDay, long currentStartTime, String srName, long count, boolean isRead) {
        String fileName;
        String curDay = String.valueOf(currentDay);
        if (isRead) {
            fileName = new StringBuilder(this.basePath)
                .append(File.separator)
                .append(READ_DIR)
                .toString();
        } else {
            fileName = new StringBuilder(this.basePath)
                .append(File.separator)
                .append(WRITE_DIR)
                .toString();
        }
        File dir = new File(fileName);
        if (!dir.isDirectory()) {
            dir.deleteOnExit();
            dir.mkdirs();
        }
        fileName = new StringBuilder(fileName)
            .append(File.separator)
            .append(curDay)
            .append(".rd")
            .toString();

        String content = new StringBuilder()
            .append(currentStartTime)
            .append(SPACE_SEPARATOR)
            .append(srName)
            .append(SPACE_SEPARATOR)
            .append(count)
            .append("\n")
            .toString();
        appendToFile(fileName, content);
    }

    public void appendToFile(String fileName, String content) {
        try (FileWriter writer = new FileWriter(fileName, true)) {
            //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件,如果为 true，则将字节写入文件末尾处，而不是写入文件开始处
            writer.write(content);
        } catch (IOException e) {
            LOG.error("record to file error");
        }
    }

    public static void main(String[] args) throws IOException {
        File rw = new File("/home/wangchao/test/1");
        FileWriter fileWriter = new FileWriter(rw, true);
        fileWriter.write("sdklfja;sldkfj;alskdfjlk-----" + "\n");
        fileWriter.flush();
    }
}
