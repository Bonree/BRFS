package com.bonree.brfs.disknode;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName TrashRecoveryResource
 * @Description
 * @Author Tang Daqian
 * @Date 2021/1/4 19:20
 **/
@Path("/trash")
public class TrashRecoveryResource {
    private static final Logger log = LoggerFactory.getLogger(TrashRecoveryResource.class);
    private StorageConfig storageConfig;
    private String trashRootDir;

    @Inject
    public TrashRecoveryResource(StorageConfig storageConfig) {
        this.storageConfig = storageConfig;
        trashRootDir = storageConfig.getTrashDir();
    }

    @GET
    @Path("fullRecovery/{srName}")
    public void reoveryAllTrashFiles(String srName) {
        String trashRootDir = storageConfig.getTrashDir();
        File snTrashDir = new File(trashRootDir, srName);
        File[] deleteTimeDirs = snTrashDir.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY);
        for (File dirNeedToRecovery : deleteTimeDirs) {
            File metadataFile = new File(dirNeedToRecovery, ".metadata");
            if (!metadataFile.exists()) {
                log.warn("do not find .metadata file in path [{}]", dirNeedToRecovery.getAbsolutePath());
                continue;
            }
            readMetadataFile(dirNeedToRecovery, true, metadataFile);
        }
    }

    @GET
    @Path("fullRecovery/{srName}")
    public void reoveryTrashFilesWithATimeStamp(@PathParam("srName")String srName,
                                                @QueryParam("timeStamp")long timeStamp) {
        File snTrashDir = new File(trashRootDir, srName);
        ArrayList<File> deleteTimeDirs = Lists.newArrayList(snTrashDir.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY));
        File dirNeedToRecovery = new File(snTrashDir,timeStamp + "");
        if (!deleteTimeDirs.contains(dirNeedToRecovery)) {
            log.warn("trash dir do not contain the dir of timestamp [{}]", timeStamp);
            return;
        }
        File metadataFile = new File(dirNeedToRecovery, ".metadata");
        if (!metadataFile.exists()) {
            log.warn("do not find .metadata file in path [{}]", dirNeedToRecovery.getAbsolutePath());
            return;
        }
        readMetadataFile(dirNeedToRecovery, true, metadataFile);
    }

    @GET
    @Path("singleDirRecovery/{srName}")
    public void reoveryTrashFilesWithATimeInterval(@PathParam("srName")String srName,
                                                   @QueryParam("lowTimeBoundary")long lowTimeBoundary,
                                                   @QueryParam("highTimeBoundary")long highTimeBoundary) {
        String trashRootDir = storageConfig.getTrashDir();
        File snTrashDir = new File(trashRootDir, srName);
        File[] deleteTimeDirs = snTrashDir.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY);
        List<File> dirNeedToRecoverys = new ArrayList<>();
        for (File deleteTimeDir : deleteTimeDirs) {
            long deleteTime = Long.parseLong(deleteTimeDir.getName());
            if (deleteTime >= lowTimeBoundary  && deleteTime <= highTimeBoundary) {
                dirNeedToRecoverys.add(deleteTimeDir);
            }
        }
        for (File dirNeedToRecovery : dirNeedToRecoverys) {
            File metadataFile = new File(dirNeedToRecovery, ".metadata");
            if (!metadataFile.exists()) {
                log.warn("do not find .metadata file in path [{}]", dirNeedToRecovery.getAbsolutePath());
                continue;
            }
            readMetadataFile(dirNeedToRecovery, true, metadataFile);
        }
    }

    private void readMetadataFile(File dirNeedToRecovery, boolean deleteSuccessFlag, File metadataFile) {
        try (RandomAccessFile randomFile = new RandomAccessFile(metadataFile, "r")) {
            long length = 1L;
            while (randomFile.length() > length) {
                String filePathBeforeDeleteString = randomFile.readLine();
                File filePathBeforeDelete = new File(filePathBeforeDeleteString);
                String fileToRecovery = filePathBeforeDelete.getName();
                File srcFile = new File(dirNeedToRecovery, fileToRecovery);
                if (srcFile.exists() && !filePathBeforeDelete.exists()) {
                    FileUtils.moveToDirectory(srcFile, filePathBeforeDelete.getParentFile(), false);
                    log.info("move [{}] to [{}]", srcFile, filePathBeforeDelete.getParent());
                }
                length += filePathBeforeDeleteString.length() + 1;
            }
        } catch (IOException e) {
            deleteSuccessFlag = false;
            log.error("failed to recovery file because of [{}]", e);
        }
        if (deleteSuccessFlag) {
            metadataFile.delete();
            dirNeedToRecovery.delete();
        }
    }

}
