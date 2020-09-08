package com.bonree.brfs.disknode.data.write;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.timer.WheelTimer;
import com.bonree.brfs.common.timer.WheelTimer.Timeout;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.ByteUtils;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.write.data.FileDecoder;
import com.bonree.brfs.common.write.data.FileEncoder;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.data.write.buf.ByteArrayFileBuffer;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.data.write.record.RecordElement;
import com.bonree.brfs.disknode.data.write.record.RecordFileBuilder;
import com.bonree.brfs.disknode.data.write.worker.RandomWriteWorkerSelector;
import com.bonree.brfs.disknode.data.write.worker.WriteTask;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.data.write.worker.WriteWorkerGroup;
import com.bonree.brfs.disknode.data.write.worker.WriteWorkerSelector;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.utils.BRFSRdFileFilter;
import com.bonree.brfs.disknode.utils.Pair;
import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import com.google.common.base.Splitter;
import com.google.common.primitives.Bytes;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWriterManager implements LifeCycle {
    private static final Logger log = LoggerFactory.getLogger(FileWriterManager.class);

    // 默认的写Worker线程数量
    private final WriteWorkerGroup workerGroup;
    private final WriteWorkerSelector workerSelector;
    private final RecordCollectionManager recorderManager;
    private final FileNodeStorer fileNodeStorer;

    private static int recordCacheSize = Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_WRITER_RECORD_CACHE_SIZE);
    private static int dataCacheSize = Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_WRITER_DATA_CACHE_SIZE);

    private final ConcurrentHashMap<String, Pair<RecordFileWriter, WriteWorker>> runningWriters = new ConcurrentHashMap<>();

    private final Duration fileIdleDuration = Duration.parse(
        Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_FILE_IDLE_TIME));
    private final WheelTimer<String> timeoutWheel = new WheelTimer<String>((int) fileIdleDuration.getSeconds());

    private final FileFormater fileFormater;

    public FileWriterManager(RecordCollectionManager recorderManager,
                             FileNodeStorer fileNodeStorer,
                             FileFormater fileFormater) {
        this(Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_WRITER_WORKER_NUM),
             recorderManager,
             fileNodeStorer,
             fileFormater);
    }

    public FileWriterManager(int workerNum,
                             RecordCollectionManager recorderManager,
                             FileNodeStorer fileNodeStorer,
                             FileFormater fileFormater) {
        this(workerNum,
             new RandomWriteWorkerSelector(),
             recorderManager,
             fileNodeStorer,
             fileFormater);
    }

    public FileWriterManager(int workerNum, WriteWorkerSelector selector,
                             RecordCollectionManager recorderManager,
                             FileNodeStorer fileNodeStorer,
                             FileFormater fileFormater) {
        this.workerGroup = new WriteWorkerGroup(workerNum);
        this.workerSelector = selector;
        this.recorderManager = recorderManager;
        this.fileNodeStorer = fileNodeStorer;
        this.fileFormater = fileFormater;
    }

    @Override
    public void start() throws Exception {
        workerGroup.start();

        timeoutWheel.setTimeout(new Timeout<String>() {

            @Override
            public void timeout(String filePath) {
                log.info("Time to flush file[{}]", filePath);

                try {
                    flushFile(filePath);
                } catch (FileNotFoundException e) {
                    log.error("flush file[{}] error", filePath, e);
                }
            }
        });
        timeoutWheel.start();
    }

    public void flushIfNeeded(String filePath) {
        timeoutWheel.update(filePath);
    }

    public void flushFile(String path) throws FileNotFoundException {
        Pair<RecordFileWriter, WriteWorker> binding = getBinding(path, false);
        if (binding == null) {
            throw new FileNotFoundException(path);
        }

        binding.second().put(new WriteTask<Void>() {

            @Override
            protected Void execute() throws Exception {
                log.info("execute flush for file[{}] BEGIN", binding.first().getPath());
                if (runningWriters.containsKey(path)) {
                    binding.first().flush();
                }

                return null;
            }

            @Override
            protected void onPostExecute(Void result) {
                log.info("execute flush for file[{}] OVER", binding.first().getPath());
            }

            @Override
            protected void onFailed(Throwable e) {
                log.error("flush error {}", path, e);
            }
        });
    }

    public void flushAll() {
        for (String filePath : runningWriters.keySet()) {
            try {
                flushFile(filePath);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public void rebuildFileWriterbyDir(String dataDirPath) {
        Map<String, String> baseMap = new HashMap<>();
        List<BRFSPath> rds = BRFSFileUtil.scanBRFSFiles(dataDirPath, baseMap, baseMap.size(), new BRFSRdFileFilter());
        File rdFile = null;
        File dataFile = null;
        for (BRFSPath path : rds) {
            rdFile = new File(new StringBuilder().append(dataDirPath).append(FileUtils.FILE_SEPARATOR).append(path).toString());
            dataFile = RecordFileBuilder.reverse(rdFile);
            if (!dataFile.exists()) {
                log.error("no data file is attached to a existed rd file[{}]!", rdFile.getAbsolutePath());
                rdFile.delete();
                continue;
            }

            try {
                rebuildFileWriter(dataFile);

                FileNode fileNode = fileNodeStorer.getFileNode(dataFile.getName());
                if (fileNode == null) {
                    log.info("file node of [{}] has been removed, close it", dataFile.getAbsolutePath());
                    closeUnexpected(dataFile.getAbsolutePath());
                }
            } catch (Throwable e) {
                log.error("rebuild file[{}] error!", dataFile.getAbsolutePath(), e);
            }
        }

    }

    @Override
    public void stop() {
        for (Entry<String, Pair<RecordFileWriter, WriteWorker>> entry : runningWriters.entrySet()) {
            try {
                entry.getValue().first().flush();
            } catch (IOException e) {
                log.error("stop to flush file[{}] error", entry.getKey(), e);
            }
        }
        workerGroup.stop();
    }

    public Pair<RecordFileWriter, WriteWorker> getBinding(String path, boolean createIfNeeded) {
        Pair<RecordFileWriter, WriteWorker> binding = runningWriters.get(path);

        if (binding != null) {
            return binding;
        }

        if (!createIfNeeded) {
            return null;
        }

        return buildDiskWriter(path);
    }

    public void rebuildFileWriter(File dataFile) throws IOException {
        log.info("rebuilding writer for file[{}]", dataFile.getAbsolutePath());
        RecordFileWriter writer = new RecordFileWriter(
            recorderManager.getRecordCollection(dataFile, true, recordCacheSize, true),
            new BufferedFileWriter(dataFile, true, new ByteArrayFileBuffer(dataCacheSize)));

        Pair<RecordFileWriter, WriteWorker> binding = new Pair<RecordFileWriter, WriteWorker>(
            writer, workerSelector.select(workerGroup.getWorkerList()));

        runningWriters.put(dataFile.getAbsolutePath(), binding);
    }

    private Pair<RecordFileWriter, WriteWorker> buildDiskWriter(String filePath) {
        Pair<RecordFileWriter, WriteWorker> binding = runningWriters.get(filePath);

        if (binding == null) {
            synchronized (runningWriters) {
                binding = runningWriters.get(filePath);
                if (binding == null) {
                    try {
                        File file = new File(filePath);
                        if (!file.getParentFile().exists()) {
                            file.getParentFile().mkdirs();
                        }

                        RecordFileWriter writer = new RecordFileWriter(
                            recorderManager.getRecordCollection(filePath, false, recordCacheSize, true),
                            new BufferedFileWriter(filePath, new ByteArrayFileBuffer(dataCacheSize)));

                        binding = new Pair<RecordFileWriter, WriteWorker>(
                            writer, workerSelector.select(workerGroup
                                                              .getWorkerList()));

                        runningWriters.put(filePath, binding);
                    } catch (Exception e) {
                        log.error("build disk writer error", e);
                    }
                }
            }
        }

        return binding;
    }

    //获取缺失或多余的日志记录信息
    private List<RecordElement> validElements(String filepath, List<RecordElement> originElements) {
        byte[] bytes = DataFileReader.readFile(filepath, 0);
        List<String> offsets = FileDecoder.getDataFileOffsets(bytes);
        log.info("adjust get [{}] records from data file of [{}]", offsets.size(), filepath);

        List<RecordElement> validElmentList = new ArrayList<RecordElement>();
        RecordElement element = originElements.get(0);
        if (element.getOffset() != 0) {
            //没有文件头的日志记录，不应该发生的
            throw new IllegalStateException("no header record in file[" + filepath + "]");
        }

        validElmentList.add(element);
        for (int index = 0; index < offsets.size(); index++) {
            List<String> parts = Splitter.on("|").splitToList(offsets.get(index));
            int offset = Integer.parseInt(parts.get(0));
            int size = Integer.parseInt(parts.get(1));
            long crc = ByteUtils.crc(bytes, offset, size);

            if (index + 1 >= originElements.size()) {
                //数据文件还有数据，但日志文件没有记录
                validElmentList.add(new RecordElement(offset, size, crc));
                continue;
            }

            element = originElements.get(index + 1);

            if (element.getOffset() != offset) {
                log.warn("excepted offset[{}], but get offset[{}] for file[{}]", offset, element.getOffset(), filepath);
                break;
            }

            if (element.getSize() != size) {
                log.warn("excepted size[{}], but get size[{}] for file[{}]", size, element.getSize(), filepath);
                break;
            }

            if (element.getCrc() != crc) {
                log.warn("excepted crc[{}], but get crc[{}] for file[{}]", crc, element.getCrc(), filepath);
                break;
            }

            validElmentList.add(element);
        }

        return validElmentList;
    }

    public void adjustFileWriter(String filePath) throws IOException {
        Pair<RecordFileWriter, WriteWorker> binding = runningWriters.get(filePath);
        if (binding == null) {
            throw new IllegalStateException("no writer of " + filePath + " is found for adjust");
        }

        List<RecordElement> originElements = binding.first().getRecordCollection().getRecordElementList();
        log.info("adjust get [{}] records from record collection of [{}]", originElements.size(), filePath);
        if (originElements.isEmpty()) {
            //没有数据写入成功，不需要任何协调
            return;
        }

        List<RecordElement> elements = validElements(filePath, originElements);
        log.info("adjust file get elements size[{}] for file[{}]", elements.size(), filePath);
        RecordElement lastElement = elements.get(elements.size() - 1);
        long validPosition = lastElement.getOffset() + lastElement.getSize();
        log.debug("last element : {}", lastElement);

        boolean needFlush = false;
        if (validPosition != binding.first().position()) {
            log.info("rewrite file content of file[{}] from[{}] to [{}]", filePath, binding.first().position(), validPosition);
            //数据文件的内容和日志信息不一致，需要调整数据文件
            binding.first().position(validPosition);
            needFlush = true;
        }

        if (elements.size() != originElements.size()) {
            log.info("rewrite file records of file[{}]", filePath);
            binding.first().getRecordCollection().clear();
            for (RecordElement element : elements) {
                binding.first().getRecordCollection().put(element);
            }
            needFlush = true;
        }

        if (needFlush) {
            binding.first().flush();
        }
    }

    private void closeUnexpected(String filePath) throws IOException {
        Pair<RecordFileWriter, WriteWorker> binding = runningWriters.remove(filePath);
        if (binding == null) {
            throw new IllegalStateException(String.format("file[%s] is not built", filePath));
        }

        binding.first().write(Bytes.concat(FileEncoder.validate(-1), FileEncoder.tail()));
        binding.first().flush();

        timeoutWheel.remove(filePath);
        CloseUtils.closeQuietly(binding.first());
    }

    public long close(String filePath) throws IOException {
        Pair<RecordFileWriter, WriteWorker> binding = runningWriters.remove(filePath);
        if (binding == null) {
            return -1;
        }

        binding.first().flush();
        byte[] fileBytes = DataFileReader.readFile(filePath, fileFormater.fileHeader().length());
        long length = fileBytes.length;
        long crcCode = ByteUtils.crc(fileBytes);
        log.info("final crc code[{}] by bytes[{}] of file[{}]", crcCode, fileBytes.length, filePath);

        binding.first().write(Bytes.concat(FileEncoder.validate(crcCode), FileEncoder.tail()));
        binding.first().flush();
        timeoutWheel.remove(filePath);
        CloseUtils.closeQuietly(binding.first());

        return length;
    }
}
