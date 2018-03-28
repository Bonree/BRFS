package com.bonree.brfs.rebalance.record;

import java.io.IOException;

public class Test {

    public static void main(String[] args) throws IOException {
        String fileName = "e:/brfstest/test";
        SimpleRecordWriter simpleWriter = new SimpleRecordWriter(fileName);
        simpleWriter.writeRecord("11111");
        simpleWriter.writeRecord("22222");
        simpleWriter.writeRecord("33333");

        simpleWriter.close();
        SimpleRecordReader simpleReader = new SimpleRecordReader(fileName);
        System.out.println(simpleReader.readerRecord());
        simpleReader.close();
    }

}
