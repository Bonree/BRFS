package com.bonree.brfs.rebalance;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月23日 下午2:13:13
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 平衡数据的接口，目前有虚拟ServerID恢复和多副本的serverID恢复
 ******************************************************************************/
public interface DataRecover {

    public enum ExecutionStatus {
        INIT, RECOVER, FINISH
    }

    public enum RecoverType {
        VIRTUAL, NORMAL
    }

    void recover();
}
