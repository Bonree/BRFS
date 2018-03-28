package com.bonree.brfs.rebalance.record;

import java.util.Date;

import org.apache.commons.lang3.time.FastDateFormat;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月28日 上午10:41:26
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 平衡文件时，所做的记录
 ******************************************************************************/
public class BalanceRecord {

    private static final String PATTERN = "yyyy-MM-dd HH:mm:ss";

    private static final String FIELD_SEPARATOR = ",";

    private String finishTime;

    private String fileName;

    private String sourceMultiId;

    private String targetMuiltiId;

    public BalanceRecord(String fileName, String sourceMultiId, String targetMuiltiId) {
        finishTime = FastDateFormat.getInstance(PATTERN).format(new Date());
        fileName = this.fileName;
        sourceMultiId = this.sourceMultiId;
        targetMuiltiId = this.targetMuiltiId;
    }

    public String getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(String finishTime) {
        this.finishTime = finishTime;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getSourceMultiId() {
        return sourceMultiId;
    }

    public void setSourceMultiId(String sourceMultiId) {
        this.sourceMultiId = sourceMultiId;
    }

    public String getTargetMuiltiId() {
        return targetMuiltiId;
    }

    public void setTargetMuiltiId(String targetMuiltiId) {
        this.targetMuiltiId = targetMuiltiId;
    }

    @Override
    public String toString() {
        return finishTime + FIELD_SEPARATOR + fileName + sourceMultiId + targetMuiltiId;
    }

}
