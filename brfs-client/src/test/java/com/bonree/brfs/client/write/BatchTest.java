package com.bonree.brfs.client.write;

import com.bonree.brfs.client.BRFS;
import com.bonree.brfs.client.BRFSClientBuilder;
import com.bonree.brfs.client.BatchResult;
import com.bonree.brfs.client.ClientConfigurationBuilder;
import com.bonree.brfs.client.ProtobufPutObjectBatch;
import com.bonree.brfs.client.PutObjectResult;
import java.net.URI;
import java.util.List;

public class BatchTest {

    public static void main(String[] args) {
        BRFS client = new BRFSClientBuilder()
            .config(new ClientConfigurationBuilder()
                        .setDataPackageSize(300)
                        .build())
            .build("root", "12345", new URI[] {URI.create("http://localhost:8200")});

        byte[] bytes = "1234567890abcd".getBytes();
        try {
            BatchResult result = client.putObjects("guice_test", ProtobufPutObjectBatch.newBuilder()
                .putObject(bytes)
                .putObject(bytes)
                .putObject(bytes)
                .build());

            List<PutObjectResult> rs = result.getResults();
            System.out.println(rs);
            rs.forEach(p -> System.out.println(p.getFID()));
        } catch (Exception e) {
            e.printStackTrace();
        }

        client.shutdown();
    }
}
