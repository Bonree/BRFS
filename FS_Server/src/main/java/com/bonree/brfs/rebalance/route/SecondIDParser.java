package com.bonree.brfs.rebalance.route;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

public class SecondIDParser {

    private int snID;
    private CuratorClient curatorClient;

    public SecondIDParser(CuratorClient curatorClient, int snID) {
        this.snID = snID;
        this.curatorClient = curatorClient;
    }
    

}
