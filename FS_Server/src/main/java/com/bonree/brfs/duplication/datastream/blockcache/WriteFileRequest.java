package com.bonree.brfs.duplication.datastream.blockcache;

import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.data.FSPacket;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;

class WriteFileRequest implements WriteRequest{
    private long cTime ;
    private FSPacket fsPacket;
    private int waitTimeOut = Configs.getConfiguration().getConfig(RegionNodeConfigs.FILE_WAIT_FOR_WRITE_TIME);
    private HandleResultCallback handleResultCallback;

    public WriteFileRequest(FSPacket fsPacket, HandleResultCallback handleResultCallback) {
        this.fsPacket = fsPacket;
        this.handleResultCallback = handleResultCallback;
        cTime = System.currentTimeMillis();
    }

    public FSPacket getFsPacket() {
        return fsPacket;
    }

    public HandleResultCallback getHandleResultCallback() {
        return handleResultCallback;
    }

    public boolean ifRequestIsTimeOut(){
        return System.currentTimeMillis() - cTime > waitTimeOut;
    }
}
