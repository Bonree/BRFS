package com.bonree.brfs.gui.server.stats;

import com.bonree.brfs.common.statistic.ReadCountModel;
import com.bonree.brfs.common.statistic.WriteCountModel;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.gui.server.TimedData;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatReportor {
    private static final Logger LOG = LoggerFactory.getLogger(StatisticCollector.class);
    private StatConfigs configs;
    private static String READ_RD;
    private static String WRITE_RD;
    private final ThreadLocal<Instant> importantMoment = new ThreadLocal<>();
    // sr=>[ts=>count]
    private final ThreadLocal<Map<String, Map<Long, Pair<ReadCountModel, WriteCountModel>>>> result = new ThreadLocal();
    private List<String> writeFiles = new ArrayList<>();

    @Inject
    public StatReportor(StatConfigs configs) {
        this.configs = configs;
        READ_RD = configs.getBaseDir() + File.separator + "read";
        WRITE_RD = configs.getBaseDir() + File.separator + "write";
        result.set(new ConcurrentHashMap<>());
    }

    public BusinessStats getCount(String srName, int minutes) {
        Instant startMoment = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(minutes, ChronoUnit.MINUTES);
        importantMoment.set(startMoment);
        getWriteCount(srName, true);
        getWriteCount(srName, false);
        return popAll(srName);
    }

    public void getWriteCount(String srName, boolean isWrite) {
        Instant now = importantMoment.get();
        File directory = new File(isWrite ? WRITE_RD : READ_RD);
        if (!directory.exists() && !directory.mkdirs()) {
            LOG.error("create stat dir error!");
            throw new InternalServerErrorException();
        }
        File[] fileList = directory.listFiles();
        if (fileList == null) {
            LOG.info("no data has been collected.");
            throw new NotFoundException("no data has been collected");
        }
        File tmp;
        Instant fileInstant;
        for (File file : fileList) {
            writeFiles.add(file.getAbsolutePath());

            System.out.println(file.getAbsolutePath());
            tmp = file;
            fileInstant = parseFromDay(getDayFromFileName(tmp.getName()));
            if (fileInstant.compareTo(now.truncatedTo(ChronoUnit.DAYS)) >= 0) {
                captureFromFile(tmp, srName, isWrite);
            }
        }
    }

    private void captureFromFile(File tmp, String srName, boolean isWrite) {
        try {
            RandomAccessFile rf = new RandomAccessFile(tmp, "r");
            long start; // 返回此文件中的当前偏移量
            long fileLength = rf.length();
            if (fileLength <= 0) {
                return;
            }
            start = rf.getFilePointer();
            long readIndex = start + fileLength - 1;
            String line;
            rf.seek(readIndex); // 设置偏移量为文件末尾
            int c;
            while (readIndex > start) {
                c = rf.read();
                if (c == '\n' || c == '\r') {
                    line = rf.readLine();
                    System.out.println(line);
                    if (line != null) {
                        extractLine(line, srName, isWrite);
                        System.out.println(line);
                    } else {
                        System.out.println(line);
                    }
                    readIndex--;
                }
                readIndex--;
                rf.seek(readIndex);
                if (readIndex == 0) { // 当文件指针退至文件开始处，输出第一行
                    extractLine(rf.readLine(), srName, isWrite);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void extractLine(String line, String srName, boolean isWrite) {
        if (line == null || line.equals("")) {
            return;
        }
        String[] s = line.split(" ");
        String moment = s[0];
        String sr = s[1];
        String count = s[2];
        Instant instant = parseFromMinute(moment);
        if (instant.compareTo(importantMoment.get()) >= 0 && (srName.equals("") || sr.equals(srName))) {
            Map<String, Map<Long, Pair<ReadCountModel, WriteCountModel>>> srMap = result.get();
            if (srMap == null) {
                srMap = new ConcurrentHashMap<>();
                result.set(srMap);
            }
            Map<Long, Pair<ReadCountModel, WriteCountModel>> tsMap =
                srMap.computeIfAbsent(srName, k -> new TreeMap<>());

            if (isWrite) {
                if (tsMap.containsKey(instant.toEpochMilli())) {
                    tsMap.get(instant.toEpochMilli()).getFirst().addReadCount(0);
                    tsMap.get(instant.toEpochMilli()).getSecond().addWriteCount(Long.parseLong(count));
                } else {
                    tsMap.put(instant.toEpochMilli(), new Pair(new ReadCountModel(0, srName),
                                                               new WriteCountModel(Long.parseLong(count))));
                }
            } else {
                if (tsMap.containsKey(instant.toEpochMilli())) {
                    tsMap.get(instant.toEpochMilli()).getFirst().addReadCount(Long.parseLong(count));
                    tsMap.get(instant.toEpochMilli()).getSecond().addWriteCount(0);
                } else {
                    tsMap.put(instant.toEpochMilli(), new Pair(new ReadCountModel(Long.parseLong(count), srName),
                                                               new WriteCountModel(0)));
                }
            }
        }
    }

    private Instant parseFromMinute(String moment) {
        return Instant.ofEpochMilli(Long.parseLong(moment));
    }

    private Instant parseFromDay(String day) {
        return Instant.ofEpochMilli(Long.parseLong(day)).atZone(ZoneId.systemDefault()).toInstant();
    }

    private String getDayFromFileName(String logName) {
        return logName.substring(0, logName.lastIndexOf(".rd"));
    }

    public List<BusinessStats> popAll() {
        ArrayList<BusinessStats> businessStats = new ArrayList<>();
        Map<String, Map<Long, Pair<ReadCountModel, WriteCountModel>>> srMap = result.get();
        if (srMap == null) {
            return new ArrayList<>();
        }
        for (String srName : srMap.keySet()) {
            Map<Long, Pair<ReadCountModel, WriteCountModel>> tsMap = srMap.get(srName);
            ArrayList<TimedData<DataStatistic>> timedDataStatistics = new ArrayList<>();
            for (long ts : tsMap.keySet()) {
                timedDataStatistics.add(new TimedData<>(ts, new DataStatistic(tsMap.get(ts).getSecond().getWriteCount(),
                                                                              tsMap.get(ts).getFirst().getReadCount())));
            }
            businessStats.add(new BusinessStats(srName, timedDataStatistics));
        }
        result.remove();
        return businessStats;
    }

    public BusinessStats popAll(String srName) {
        Map<String, Map<Long, Pair<ReadCountModel, WriteCountModel>>> srMap = result.get();
        if (srMap == null) {
            return new BusinessStats(srName, ImmutableList.of());
        }

        Map<Long, Pair<ReadCountModel, WriteCountModel>> tsMap = srMap.get(srName);
        ArrayList<TimedData<DataStatistic>> timedDataStatistics = new ArrayList<>();
        for (long ts : tsMap.keySet()) {
            timedDataStatistics.add(new TimedData<>(ts, new DataStatistic(tsMap.get(ts).getSecond().getWriteCount(),
                                                                          tsMap.get(ts).getFirst().getReadCount())));
        }
        result.remove();
        return new BusinessStats(srName, timedDataStatistics);

    }
}
