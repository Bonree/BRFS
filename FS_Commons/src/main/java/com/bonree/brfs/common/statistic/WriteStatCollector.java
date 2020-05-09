package com.bonree.brfs.common.statistic;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WriteStatCollector extends StatisticCollector {
    private static final String STAT_DIR = "/home/wangchao/brfs/logs/statistic";
    private static final String WRITE_PREFIX = "write";
    private List<String> writeFiles = new ArrayList<>();
    private ThreadLocal<Map<Long, Integer>> resultMap = new ThreadLocal();
    private ThreadLocal<Instant> importantMoment = new ThreadLocal<>();

    public WriteStatCollector() {
        resultMap.set(new ConcurrentHashMap<>());
    }

    public Map<String, Integer> getWriteCount(int minutes, String srName) {
        Instant startMoment = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(minutes, ChronoUnit.MINUTES);
        importantMoment.set(startMoment);
        File directory = new File(STAT_DIR);
        File[] filelist = directory.listFiles();
        File tmp;
        Instant fileInstant;
        for (int i = 0; i < filelist.length; i++) {
            writeFiles.add(filelist[i].getAbsolutePath());
            System.out.println(filelist[i].getAbsolutePath());
            tmp = filelist[i];
            if (tmp.getName().startsWith(WRITE_PREFIX)) {
                fileInstant = parseFromDay(getDayFromFileName(tmp.getName()));
                if (fileInstant.atZone(ZoneId.systemDefault())
                               .compareTo(startMoment.atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS)) >= 0) {
                    captureFromFile(tmp, srName);
                }
            }
        }
        return null;
    }

    /**
     * 提取写次数放入结果集
     * @param tmp
     */
    private void captureFromFile(File tmp, String srName) {
        try {
            RandomAccessFile rf = new RandomAccessFile(tmp, "r");
            long start = 0; // 返回此文件中的当前偏移量
            long fileLength = rf.length();
            if (fileLength <= 0) {
                return;
            }
            start = rf.getFilePointer();
            long readIndex = start + fileLength - 1;
            String line;
            rf.seek(readIndex); // 设置偏移量为文件末尾
            int c = -1;
            while (readIndex > start) {
                c = rf.read();
                if (c == '\n' || c == '\r') {
                    line = rf.readLine();
                    System.out.println(line);
                    if (line != null) {
                        extractLine(line, srName);
                        System.out.println(line);
                    } else {
                        System.out.println(line);
                    }
                    readIndex--;
                }
                readIndex--;
                rf.seek(readIndex);
                if (readIndex == 0) { // 当文件指针退至文件开始处，输出第一行
                    extractLine(rf.readLine(), srName);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        resultMap.get();
    }

    private void extractLine(String line, String srName) {
        if (line.equals("") || line == null) {
            return;
        }
        String moment = line.substring(0, 16);
        String sr = line.split(" ")[2];
        Instant instant = parseFromMinute(moment);
        if (instant.compareTo(importantMoment.get()) >= 0 && sr.equals(srName)) {
            Map<Long, Integer>  result = resultMap.get();
            if (result.get(instant.toEpochMilli()) == null) {
                result.put(instant.toEpochMilli(), 0);
            }
            int count = result.get(instant.toEpochMilli()).intValue() + 1;
            result.put(instant.toEpochMilli(), count);
        }
    }

    private Instant parseFromMinute(String date) {
        Instant moment = LocalDateTime.parse(
            date,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(ZoneId.systemDefault())
        ).atZone(ZoneId.systemDefault()).toInstant();
        return moment;
    }

    private Instant parseFromDay(String day) {
        Instant moment = LocalDate.parse(
            day,
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault())
        ).atStartOfDay(ZoneId.systemDefault()).toInstant();
        return moment;
    }

    private String getDayFromFileName(String logName) {
        return logName.substring(16, 26);
    }

    public static void main(String[] args) throws ParseException {
        System.out.println(new WriteStatCollector().getDayFromFileName("write-statistic-2020-04-29.log"));
        WriteStatCollector writeStatCollector = new WriteStatCollector();
        writeStatCollector.getWriteCount(30 + (24 * 60), "new_reigon7");
        writeStatCollector.checkResult();
    }

    private void checkResult() {
        Map<Long, Integer> map = resultMap.get();
        map.forEach((key, value) -> {
            System.out.println(key + ":" + value);
        });
    }
}
