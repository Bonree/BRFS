package com.bonree.brfs.rebalance.task;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.UUID;

public class NodeMonitorTest {

    public static void main(String[] args) {
        // NodeCache nodeCache = new NodeCache(client, path);
        // nodeCache.close();

        // CuratorCacheFactory.init("192.168.101.86:2181");
        // CuratorNodeCache nodeCache = CuratorCacheFactory.getNodeCache();
        // nodeCache.addListener("/zcg/tets", new AbstractNodeCacheListener("test") {
        //
        // @Override
        // public void nodeChanged() throws Exception {
        // System.out.println("11111");
        // }
        // });
        File file = new File("e:/fdfs_data/small.txt");
        if (file.isDirectory()) {
            throw new IllegalArgumentException("fileName not is directory");
        }
        OutputStreamWriter writer = null;
        BufferedWriter bw = null;
        try {
            writer = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
            bw = new BufferedWriter(writer);
            for (int i = 0; i < 100; i++) {
                String record = UUID.randomUUID().toString();
                bw.write(record + "\n");
            }
            bw.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

}
