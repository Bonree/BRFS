package com.bonree.brfs.tasks.maintain;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.disknode.StorageConfig;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName TrashMaintainer
 * @Description 检查垃圾桶的文件是否需要清理的任务
 * @Author Tang Daqian
 * @Date 2020/12/24 11:55
 **/
@ManageLifecycle
public class TrashMaintainer implements LifeCycle {

    private static final Logger log = LoggerFactory.getLogger(TrashMaintainer.class);

    private StorageConfig storageConfig;
    private String trashPathString;
    private String pattern = "^\\d{13}$";
    private final ScheduledExecutorService trashExec;

    @Inject
    public TrashMaintainer(StorageConfig storageConfig) {
        this.storageConfig = storageConfig;
        this.trashPathString = storageConfig.getTrashDir();
        this.trashExec = Executors
            .newSingleThreadScheduledExecutor(makeThreadFactory("TrashMaintainer", 1));
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        log.info("start period thread for trash can.");
        trashExec.scheduleAtFixedRate(() -> {
            // 检查.trash文件夹的目录文件
            File trashDir = new File(trashPathString);
            if (!trashDir.isDirectory()) {
                return;
            }
            // 获取垃圾桶目录下sr目录
            File[] srDirs = trashDir.listFiles();
            long currentTime = System.currentTimeMillis();
            for (File srDir : srDirs) {
                // 如果不是目录跳过检查
                if (!srDir.isDirectory()) {
                    continue;
                }
                // 获取sr的删除日期目录
                File[] deleteTimeDirs = srDir.listFiles();
                for (File deleteTimeDir : deleteTimeDirs) {
                    // 如果是目录但是不是时间戳格式，跳过检查
                    if (!Pattern.matches(pattern, deleteTimeDir.getName())) {
                        continue;
                    }
                    // 检查目录，获取创建时间，如果时间大于设置的垃圾清理时间，则删除文件，
                    int createTime = Integer.parseInt(deleteTimeDir.getName());
                    try {
                        if ((currentTime - createTime) >= Configs.getConfiguration().getConfig(
                            DataNodeConfigs.CLEAN_TRASH_INTERVAL) * 24 * 60 * 60 * 1000) {
                            log.info("Remove file [{}] completely, delete this file from trash.", srDir.getName());
                            FileUtils.deleteDirectory(deleteTimeDir);
                        }
                    } catch (IOException e) {
                        log.error("failed to remove file [{}] from trash beacuse of [{}].", srDir.getName(), e);
                    }
                }
            }
        }, 1, 1, TimeUnit.MINUTES);

    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if (trashExec != null) {
            trashExec.shutdownNow();
        }
        log.info("Trash server will be stopped!");
    }

    /**
     * 删除文件移动到垃圾桶
     */
    public boolean moveFileToTrash(Map<String, List<File>> fileToDelete) {

        long currentTime = System.currentTimeMillis();
        String trashTime = "";
        RandomAccessFile metaDataFile = null;
        try {
            for (Map.Entry<String, List<File>> entry : fileToDelete.entrySet()) {
                // 删除目录结构 .trash/storageRegionName/删除日期
                trashTime = trashPathString + "/" + entry.getKey() + "/" + currentTime;
                File trashTimeDir = new File(trashTime);
                if (!trashTimeDir.exists()) {
                    if (!trashTimeDir.isDirectory()) {
                        trashTimeDir.mkdirs();
                    }
                }
                List<File> deleteFiles = entry.getValue();
                if (deleteFiles == null || deleteFiles.size() == 0) {
                    continue;
                }
                File metaData = new File(trashTimeDir, ".metadata");
                if (!metaData.exists()) {
                    metaData.createNewFile();
                }
                metaDataFile = new RandomAccessFile(metaData, "rw");
                for (File deleteFile : deleteFiles) {
                    log.info("Moved: '" + deleteFile.getAbsolutePath() + "' to trash at: " + trashTimeDir);
                    FileUtils.moveToDirectory(deleteFile, trashTimeDir, true);
                    metaDataFile.seek(metaDataFile.length());
                    metaDataFile.writeBytes(deleteFile.getAbsolutePath() + "\n");
                }
            }
            metaDataFile.close();
            return true;
        } catch (IOException e) {
            log.error("Failed to move to trash because of [{}]", e);
            try {
                metaDataFile.close();
            } catch (IOException ioException) {
                log.error("failed to close RandomAccessFile beacuse of [{}]", ioException);
            }
            return false;
        }
    }

    private static ThreadFactory makeThreadFactory(@NotNull String nameFormat, @Nullable Integer priority) {
        final ThreadFactoryBuilder builder = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(nameFormat);
        if (priority != null) {
            builder.setPriority(priority);
        }

        return builder.build();
    }
}