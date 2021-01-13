package com.bonree.brfs.disknode.trash.recovery;

import com.bonree.brfs.client.utils.SocketChannelSocketFactory;
import com.bonree.brfs.common.http.HttpServerConfig;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.disknode.StorageConfig;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName RecoveryFileFromTrash
 * @Description
 * @Author Tang Daqian
 * @Date 2021/1/5 16:01
 **/
public class RecoveryFileFromTrashManager {

    private static final Logger log = LoggerFactory.getLogger(RecoveryFileFromTrashManager.class);

    private ScheduledExecutorService trashRecoveryExec;
    private static String trashRootDir;
    private AtomicBoolean isExecute = new AtomicBoolean(false);
    private Future<?> future;
    private ServiceManager serviceManager;
    private HttpServerConfig config;
    private static OkHttpClient httpClient = new OkHttpClient.Builder()
        .addNetworkInterceptor(
            chain -> chain.proceed(chain.request().newBuilder().addHeader("Expect", "100-continue").build()))
        .socketFactory(new SocketChannelSocketFactory())
        .build();

    @Inject
    public RecoveryFileFromTrashManager(StorageConfig storageConfig,
                                        ServiceManager serviceManager,
                                        HttpServerConfig config) {
        trashRootDir = storageConfig.getTrashDir();
        this.trashRecoveryExec = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("RecoveryFileFromTrashManager").build());
        this.serviceManager = serviceManager;
        this.config = config;
    }

    public void recovery(Runnable runnable, TrashRecoveryCallBack callBack) {
        if (isExecute.get()) {
            callBack.error(new Exception("recovery task is executed, can not submit current task"));
            return;
        }
        isExecute.set(true);
        future = trashRecoveryExec.submit(runnable);
    }

    public void reoveryAllTrashFilesForStorageRegion(String srName, long timeStamp, TrashRecoveryCallBack callBack) {
        try {
            if (timeStamp != 0) {
                reoveryTrashFilesWithTimeStamp(srName, timeStamp, callBack);
                return;
            }

            File snTrashDir = new File(trashRootDir, srName);
            log.info("sn dir is [{}].", snTrashDir.getAbsolutePath());
            if (!snTrashDir.exists() || snTrashDir.isFile()) {
                log.warn("the storageRegionName [{}] is not exists.", srName);
                callBack.error(new Exception(srName + " is not exists."));
                return;
            }
            File[] deleteTimeDirs = snTrashDir.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY);
            for (File dirNeedToRecovery : deleteTimeDirs) {
                File metadataFile = new File(dirNeedToRecovery, ".metadata");
                log.info("metadataFile path is [{}]", metadataFile.getAbsolutePath());
                if (!metadataFile.exists()) {
                    log.warn("do not find .metadata file in path [{}]", dirNeedToRecovery.getAbsolutePath());
                    continue;
                }
                reoveryFile(dirNeedToRecovery, true, metadataFile, callBack);
            }
            callBack.complete();
        } catch (Exception e) {
            log.error("failed to recovery trash files because of [{}]", e);
            callBack.error(new Exception("failed to recovery trash files!"));
        } finally {
            setExecuteStatus(false);
        }
    }

    public void reoveryTrashFilesWithTimeInterval(String srName,
                                                  long lowTimeBoundary,
                                                  long highTimeBoundary,
                                                  TrashRecoveryCallBack callBack) {
        try {
            File snTrashDir = new File(trashRootDir, srName);
            if (!snTrashDir.exists() && snTrashDir.isFile()) {
                log.warn("the storageRegionName [{}] is not exists, plesae check your input.", srName);
                return;
            }
            File[] deleteTimeDirs = snTrashDir.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY);
            List<File> dirNeedToRecoverys = new ArrayList<>();
            for (File deleteTimeDir : deleteTimeDirs) {
                long deleteTime = Long.parseLong(deleteTimeDir.getName());
                if (deleteTime >= lowTimeBoundary && deleteTime <= highTimeBoundary) {
                    dirNeedToRecoverys.add(deleteTimeDir);
                }
            }
            for (File dirNeedToRecovery : dirNeedToRecoverys) {
                File metadataFile = new File(dirNeedToRecovery, ".metadata");
                if (!metadataFile.exists()) {
                    log.warn("do not find .metadata file in path [{}]", dirNeedToRecovery.getAbsolutePath());
                    continue;
                }
                reoveryFile(dirNeedToRecovery, true, metadataFile, callBack);
            }
            callBack.complete();
        } catch (Exception e) {
            log.error("failed to recovery trash files because of [{}]", e);
            callBack.error(new Exception("failed to recovery trash files!"));
        } finally {
            setExecuteStatus(false);
        }
    }

    private void reoveryTrashFilesWithTimeStamp(String srName, long timeStamp, TrashRecoveryCallBack callBack) {
        File snTrashDir = new File(trashRootDir, srName);
        if (!snTrashDir.exists() && snTrashDir.isFile()) {
            log.warn("the storageRegionName [{}] is not exists, plesae check your input.", srName);
            return;
        }
        ArrayList<File> deleteTimeDirs = Lists.newArrayList(snTrashDir.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY));
        File dirNeedToRecovery = new File(snTrashDir, timeStamp + "");
        if (!deleteTimeDirs.contains(dirNeedToRecovery)) {
            log.warn("trash dir do not contain the dir of timestamp [{}]", dirNeedToRecovery.getName());
            return;
        }
        File metadataFile = new File(dirNeedToRecovery, ".metadata");
        if (!metadataFile.exists()) {
            log.warn("do not find .metadata file in path [{}]", dirNeedToRecovery.getAbsolutePath());
            return;
        }
        reoveryFile(dirNeedToRecovery, true, metadataFile, callBack);
    }

    /**
     * 恢复垃圾桶文件至原来目录下
     * 其中 垃圾桶中的文件名结构为： UUID + 文件名， 无分隔符
     *
     * @param dirNeedToRecovery   垃圾桶中需要恢复的文件夹
     * @param recoverySuccessFlag 是否成功恢复标记
     * @param metadataFile        元数据文件
     */
    private void reoveryFile(File dirNeedToRecovery,
                             boolean recoverySuccessFlag,
                             File metadataFile,
                             TrashRecoveryCallBack callBack) {
        try (RandomAccessFile randomFile = new RandomAccessFile(metadataFile, "r")) {
            long length = 1L;
            while (randomFile.length() > length) {
                // 前缀为36位UUID的文件全路径
                String uniqueIdAndFilePathBeforeDeleteString = randomFile.readLine();
                String filePathBeforeDeleteString = uniqueIdAndFilePathBeforeDeleteString.substring(36);
                String uniqueId = uniqueIdAndFilePathBeforeDeleteString.substring(0, 36);
                // 文件被删除之前的存储路径
                File filePathBeforeDelete = new File(filePathBeforeDeleteString);
                // 获取当前行元数据对应的文件
                File uniqueIdAndDeletedFile = new File(dirNeedToRecovery, uniqueId + filePathBeforeDelete.getName());
                // 垃圾桶中有要恢复的文件，恢复的目标路径无该文件，进行恢复
                if (uniqueIdAndDeletedFile.exists() && !filePathBeforeDelete.exists()) {
                    if (uniqueIdAndDeletedFile.isFile()) {
                        FileUtils.moveFile(uniqueIdAndDeletedFile, filePathBeforeDelete);
                    } else {
                        FileUtils.moveToDirectory(uniqueIdAndDeletedFile,
                                                  filePathBeforeDelete.getParentFile(),
                                                  false);
                        File tmpFile = new File(filePathBeforeDelete.getParentFile(), uniqueIdAndDeletedFile.getName());
                        tmpFile.renameTo(filePathBeforeDelete);
                    }
                    log.info("Moved: '"
                                 + uniqueIdAndDeletedFile.getAbsolutePath()
                                 + "' to dir at: " + filePathBeforeDelete);
                }

                length += uniqueIdAndFilePathBeforeDeleteString.length() + 1;
            }
        } catch (IOException e) {
            recoverySuccessFlag = false;
            log.error("failed to recovery file because of [{}]", e);
        }
        if (recoverySuccessFlag) {
            metadataFile.delete();
            dirNeedToRecovery.delete();
        } else {
            callBack.error(new Exception("recovery file failed, please try again later"));
        }
    }

    public void recoveryFileForAllNode() {
        List<Service> datanodes = serviceManager
            .getServiceListByGroup(Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME));
        String uri = "";
        for (Service datanode : datanodes) {
            uri = "http://" + datanode.getHost() + ":" + config.getPort();
            Request httpRequest = new Request.Builder()
                .url(HttpUrl.get(uri)
                            .newBuilder()
                            .encodedPath("/trash/fullRecovery")
                            .build())
                .get()
                .build();

            try {
                httpClient.newCall(httpRequest).execute();
            } catch (IOException e) {
                log.error("exception happend when request for [{}] because of error [{}]", uri, e);
            }
        }
    }

    public void recoveyFileForAllNodeBySrName(String srName) {
        List<Service> datanodes = serviceManager
            .getServiceListByGroup(Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME));
        String uri = "";
        for (Service datanode : datanodes) {
            uri = "http://" + datanode.getHost() + ":" + config.getPort();
            Request httpRequest = new Request.Builder()
                .url(HttpUrl.get(uri)
                            .newBuilder()
                            .encodedPath("/trash/fullRecovery")
                            .addEncodedPathSegment(srName)
                            .build())
                .get()
                .build();
            try {
                httpClient.newCall(httpRequest).execute();
            } catch (IOException e) {
                log.error("exception happend when request for [{}] because of error [{}]", uri, e);
            }
        }
    }

    public void setExecuteStatus(boolean flag) {
        isExecute.set(flag);
    }
}
