package com.bonree.brfs.disknode.server.handler;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.write.data.FileDecoder;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.record.RecordCollection;
import com.bonree.brfs.disknode.data.write.record.RecordElement;
import com.bonree.brfs.disknode.data.write.record.RecordElementReader;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.utils.Pair;
import com.google.common.base.Splitter;
import com.google.common.primitives.Longs;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLengthMessageHandler implements MessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FileLengthMessageHandler.class);

    private DiskContext context;
    private FileWriterManager writerManager;
    private FileFormater fileFormater;

    private static final int DEFAULT_POOL_SIZE = 3;
    private ExecutorService threadPool =
        Executors.newFixedThreadPool(DEFAULT_POOL_SIZE, new PooledThreadFactory("sequence_cache"));

    public FileLengthMessageHandler(DiskContext context, FileWriterManager writerManager, FileFormater fileFormater) {
        this.context = context;
        this.writerManager = writerManager;
        this.fileFormater = fileFormater;
    }

    @Override
    public void handle(HttpMessage msg, HandleResultCallback callback) {
        LOG.info("GET sequences of file[{}]", msg.getPath());
        String filePath = context.getConcreteFilePath(msg.getPath());

        threadPool.submit(new Runnable() {

            @Override
            public void run() {
                List<RecordElement> recordInfo = getRecordElements(filePath);

                if (recordInfo == null) {
                    LOG.info("can not get record elements of file[{}]", filePath);
                    callback.completed(new HandleResult(false));
                    return;
                }

                //获取所有文件序列号
                RecordElement lastElement = null;
                if (recordInfo != null) {
                    for (RecordElement element : recordInfo) {
                        if (lastElement == null) {
                            lastElement = element;
                            continue;
                        }

                        if (lastElement.getOffset() < element.getOffset()) {
                            lastElement = element;
                        }
                    }
                }

                if (lastElement == null) {
                    LOG.info("no available record element of file[{}]", filePath);
                    callback.completed(new HandleResult(false));
                    return;
                }

                HandleResult result = new HandleResult(true);
                long fileLength = fileFormater.relativeOffset(lastElement.getOffset()) + lastElement.getSize();

                LOG.info("get file length[{}] from file[{}]", fileLength, filePath);
                result.setData(Longs.toByteArray(fileLength));
                callback.completed(result);
            }
        });
    }

    private List<RecordElement> getRecordElements(String filePath) {
        List<RecordElement> recordInfo = null;
        try {
            Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(filePath, false);
            if (binding != null) {
                RecordElementReader recordReader = null;
                try {
                    binding.first().flush();
                    writerManager.adjustFileWriter(filePath);

                    RecordCollection recordSet = binding.first().getRecordCollection();

                    recordReader = recordSet.getRecordElementReader();

                    recordInfo = new ArrayList<RecordElement>();
                    for (RecordElement element : recordReader) {
                        recordInfo.add(element);
                    }
                } catch (Exception e) {
                    LOG.error("getSequnceNumbers from file[{}] error", filePath, e);
                } finally {
                    CloseUtils.closeQuietly(recordReader);
                }
            } else {
                //到这有两种情况：
                //1、文件打开操作未成功后进行同步；
                //2、文件关闭操作未成功进行再次关闭;
                recordInfo = new ArrayList<RecordElement>();
                File dataFile = new File(filePath);
                if (dataFile.exists()) {
                    //到这的唯一机会是，多副本文件关闭时只有部分关闭成功，当磁盘节点恢复正常
                    //后，需要再次进行同步流程让所有副本文件关闭，因为没有日志文件，所以只能
                    //通过解析数据文件生成序列号列表
                    byte[] bytes = DataFileReader.readFile(dataFile);
                    List<String> offsetInfos = FileDecoder.getDataFileOffsets(fileFormater.fileHeader().length(), bytes);
                    for (String info : offsetInfos) {
                        List<String> parts = Splitter.on('|').splitToList(info);
                        int offset = Integer.parseInt(parts.get(0));
                        int size = Integer.parseInt(parts.get(1));
                        recordInfo.add(new RecordElement(offset, size, 0));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("get record element error", e);
        }

        return recordInfo;
    }

    @Override
    public boolean isValidRequest(HttpMessage message) {
        return true;
    }
}
