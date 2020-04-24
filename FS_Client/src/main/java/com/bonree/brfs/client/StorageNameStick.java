package com.bonree.brfs.client;

import java.io.Closeable;
import java.text.ParseException;
import org.joda.time.DateTime;

public interface StorageNameStick extends Closeable {
    String[] writeData(InputItem[] itemArrays);

    String writeData(InputItem item);

    InputItem readData(String fid) throws Exception;

    /**
     * 概述：
     *
     * @param startTime 开始时间 格式为 yyyy-MM-dd HH:mm:ss
     * @param endTime   结束时间 格式为 yyyy-MM-dd HH:mm:ss
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean deleteData(String startTime, String endTime);

    /**
     * 概述：
     *
     * @param startTime  开始时间
     * @param endTime    结束时间
     * @param dateForamt
     *
     * @return
     *
     * @throws ParseException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean deleteData(String startTime, String endTime, String dateForamt) throws ParseException;

    /**
     * 概述：
     *
     * @param startTime 开始时间，单位为ms
     * @param endTime   结束时间，单位为ms
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean deleteData(long startTime, long endTime);

    boolean deleteData(DateTime startTime, DateTime endTime);
}
