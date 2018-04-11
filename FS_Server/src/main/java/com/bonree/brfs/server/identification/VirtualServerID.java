package com.bonree.brfs.server.identification;

import java.util.List;

public class VirtualServerID {
    private ServerIDGen serverIdGen;

    public VirtualServerID(ServerIDGen serverIdGen) {
        this.serverIdGen = serverIdGen;
    }

    public List<String> getServerId(int count) {
        List<String> virtualServerIds = serverIdGen.getVirtualIdentification(count);
        return virtualServerIds;
    }

}
