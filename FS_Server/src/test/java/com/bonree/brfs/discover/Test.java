package com.bonree.brfs.discover;

import com.bonree.brfs.server.sequence.StorageSequenceGenetor;

public class Test {

    public static void main(String[] args) {
        StorageSequenceGenetor sequenceGen = StorageSequenceGenetor.getInstance("192.168.101.86:2181", "/brfs/wz/storageIndex");
        sequenceGen.resetSequence();
        for (int i = 0; i < 10; i++) {
            System.out.println(sequenceGen.getIncreSequence());
        }
    }

}
