package com.bonree.brfs.client;

import com.bonree.brfs.common.proto.FileDataProtos;
import com.bonree.brfs.common.write.data.FidDecoder;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * Unit test for simple App.
 */
public class AppTest {
    public static void main(String[] args) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(new File("/root/temp/brfs/read1k")));
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("/root/temp/brfs/uuids")));

        String line = null;
        int lineNum = 1;
        int errorCount = 0;
        while ((line = reader.readLine()) != null) {
            String fid = line.split(",")[0];

            try {
                FileDataProtos.Fid fidObj = FidDecoder.build(fid);
                writer.write(fidObj.getUuid() + "\n");
            } catch (Exception e) {
                System.out.println(lineNum + " : " + fid);
                errorCount++;
                //                e.printStackTrace();
                //                break;
            } finally {
                lineNum++;
            }
        }

        System.out.println("error : " + errorCount);

        reader.close();
        writer.flush();
        writer.close();
    }
}
