package com.bonree.brfs.disknode.trash.recovery;

/**
 * @ClassName TrashRecoveryCallBack
 * @Description
 * @Author Tang Daqian
 * @Date 2021/1/5 18:36
 **/
public interface TrashRecoveryCallBack {
    void complete();

    void error(Throwable cause);
}
