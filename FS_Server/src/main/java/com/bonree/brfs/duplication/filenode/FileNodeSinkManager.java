package com.bonree.brfs.duplication.filenode;

import com.bonree.brfs.common.process.LifeCycle;

/**
 * 管理{@link FileNodeSink}的接口
 *
 * @author root
 */
public interface FileNodeSinkManager extends LifeCycle {
    /**
     * 注册一个文件接受槽
     *
     * @param sink
     *
     * @throws Exception
     */
    void registerFileNodeSink(FileNodeSink sink);

    /**
     * 注销文件接受槽
     *
     * @param sink
     *
     * @throws Exception
     */
    void unregisterFileNodeSink(FileNodeSink sink);

    /**
     * 设置SinkManger的状态监听器
     *
     * @param litener
     */
    void addStateListener(StateListener litener);

    void removeStateListener(StateListener listener);

    /**
     * SinkManager的状态监听接口
     *
     * @author yupeng
     */
    public static interface StateListener {
        /**
         * Manager状态变化
         */
        void stateChanged(boolean enable);
    }
}
