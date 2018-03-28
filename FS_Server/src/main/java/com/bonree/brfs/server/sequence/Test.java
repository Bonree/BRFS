package com.bonree.brfs.server.sequence;

public class Test {

    public static void main(String[] args) {
        StorageSequenceGenetor sequenceGen=StorageSequenceGenetor.getInstance("192.168.101.86:2181", "/brfs/wz/storageIndex");
        for(int i=0;i<1000;i++) {
            System.out.println(sequenceGen.getIncreSequence());
        }
    }

}
