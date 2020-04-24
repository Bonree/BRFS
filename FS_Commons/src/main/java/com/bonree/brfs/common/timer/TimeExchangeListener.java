package com.bonree.brfs.common.timer;

import java.time.Duration;

public interface TimeExchangeListener {
    /**
     * 时间段发生切换时触发
     *
     * @param startTime 时间段的开始时间
     * @param duration  时间段
     */
    void timeExchanged(long startTime, Duration duration);
}
