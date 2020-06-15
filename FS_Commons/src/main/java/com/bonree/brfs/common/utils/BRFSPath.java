package com.bonree.brfs.common.utils;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BRFSPath {
    private static final Logger LOG = LoggerFactory.getLogger(BRFSPath.class);
    public static final String FILE_SEPARATOR = "/";
    public static final String STORAGEREGION = "SOTRAGE_REGION";
    public static final String INDEX = "INDEX";
    public static final String YEAR = "YEAR";
    public static final String MONTH = "MONTH";
    public static final String DAY = "DAY";
    public static final String TIME = "TIME";
    public static final String FILE = "FILE";
    public static final List<String> PATHLIST = Arrays.asList(new String[] {STORAGEREGION, INDEX, YEAR, MONTH, DAY, TIME, FILE});

    private static final DateTimeFormatter yearDate = DateTimeFormat.forPattern("yyyy");
    private static final DateTimeFormatter yearMonth = DateTimeFormat.forPattern("yyyy-MM");
    private static final DateTimeFormatter yearMonthDay = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final DateTimeFormatter yearMonthDayTime = DateTimeFormat.forPattern("yyyy-MM-dd HH_mm_ss");

    private String storageRegion = null;
    private String index = null;
    private String year = null;
    private String month = null;
    private String day = null;
    private String hourMinSecond = null;
    private String fileName = null;

    public String getStorageRegion() {
        return storageRegion;
    }

    public void setStorageRegion(String storageRegion) {
        this.storageRegion = storageRegion;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getHourMinSecond() {
        return hourMinSecond;
    }

    public void setHourMinSecond(String hourMinSecond) {
        this.hourMinSecond = hourMinSecond;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public static BRFSPath getInstance(String storageRegion, String index, String year, String month, String day,
                                       String hourMinSecond, String fileName) {
        BRFSPath ele = new BRFSPath();
        ele.setStorageRegion(storageRegion);
        ele.setIndex(index);
        ele.setYear(year);
        ele.setMonth(month);
        ele.setDay(day);
        ele.setHourMinSecond(hourMinSecond);
        ele.setFileName(fileName);
        return ele;
    }

    public static BRFSPath parserFile(String relativePath) {
        BRFSPath ele = new BRFSPath();
        String[] fields = StringUtils.split(relativePath, "/");
        if (fields == null || fields.length < 7) {
            return null;
        }
        int len = fields.length;
        ele.setFileName(fields[len - 1]);
        ele.setHourMinSecond(fields[len - 2]);
        ele.setDay(fields[len - 3]);
        ele.setMonth(fields[len - 4]);
        ele.setYear(fields[len - 5]);
        ele.setIndex(fields[len - 6]);
        ele.setStorageRegion(fields[len - 7]);
        return ele;
    }

    public BRFSPath copy() {
        BRFSPath ele = new BRFSPath();
        ele.setStorageRegion(this.storageRegion);
        ele.setIndex(this.index);
        ele.setYear(this.year);
        ele.setMonth(this.month);
        ele.setDay(this.day);
        ele.setHourMinSecond(this.hourMinSecond);
        ele.setFileName(this.fileName);
        return ele;
    }

    public String toString() {
        StringBuilder path = new StringBuilder();
        if (storageRegion != null) {
            path.append(storageRegion);
        } else {
            return path.toString();
        }
        if (index != null) {
            path.append("/").append(index);
        } else {
            return path.toString();
        }
        if (year != null) {
            path.append("/").append(year);
        } else {
            return path.toString();
        }
        if (month != null) {
            path.append("/").append(month);
        } else {
            return path.toString();
        }
        if (day != null) {
            path.append("/").append(day);
        } else {
            return path.toString();
        }
        if (hourMinSecond != null) {
            path.append("/").append(hourMinSecond);
        } else {
            return path.toString();
        }
        if (fileName != null) {
            path.append("/").append(fileName);
        }
        return path.toString();
    }

    public static BRFSPath getInstance(Map<String, String> content) {
        if (content == null || content.isEmpty()) {
            return null;
        }
        BRFSPath obj = new BRFSPath();
        if (content.containsKey(STORAGEREGION)) {
            obj.setStorageRegion(content.get(STORAGEREGION));
        } else {
            return obj;
        }

        if (content.containsKey(INDEX)) {
            obj.setIndex(content.get(INDEX));
        } else {
            return obj;
        }

        if (content.containsKey(YEAR)) {
            obj.setYear(content.get(YEAR));
        } else {
            return obj;
        }

        if (content.containsKey(MONTH)) {
            obj.setMonth(content.get(MONTH));
        } else {
            return obj;
        }

        if (content.containsKey(DAY)) {
            obj.setDay(content.get(DAY));
        } else {
            return obj;
        }

        if (content.containsKey(TIME)) {
            obj.setHourMinSecond(content.get(TIME));
        } else {
            return obj;
        }
        if (content.containsKey(FILE)) {
            obj.setFileName(content.get(FILE));
        }
        return obj;
    }

    public long toTimeMile() {
        Map<String, String> timeMap = new HashMap<>();

        if (year != null) {
            timeMap.put(YEAR, year);
        }
        if (month != null) {
            timeMap.put(MONTH, month);
        }
        if (day != null) {
            timeMap.put(DAY, day);
        }
        if (hourMinSecond != null) {
            timeMap.put(TIME, hourMinSecond);
        }
        return convertTime(timeMap);
    }

    public static long convertTime(Map<String, String> map) {
        StringBuilder timestr = new StringBuilder();
        String time = null;
        try {
            if (map.containsKey(YEAR)) {
                timestr.append(map.get(YEAR));
            } else {
                return Long.MAX_VALUE;
            }
            if (map.containsKey(MONTH)) {
                timestr.append("-").append(map.get(MONTH));
            } else {
                time = timestr.toString();
                return yearDate.parseDateTime(time).getMillis();

            }
            if (map.containsKey(BRFSPath.DAY)) {
                timestr.append("-").append(map.get(BRFSPath.DAY));
            } else {
                time = timestr.toString();
                return yearMonth.parseDateTime(time).getMillis();

            }

            if (map.containsKey(TIME)) {
                timestr.append(" ").append(map.get(TIME));
            } else {
                time = timestr.toString();
                return yearMonthDay.parseDateTime(time).getMillis();
            }
            time = timestr.toString();
            return yearMonthDayTime.parseDateTime(time).getMillis();
        } catch (Exception e) {
            LOG.info("invail path : {}", timestr.toString(), e);
            return Long.MAX_VALUE;
        }
    }
}
