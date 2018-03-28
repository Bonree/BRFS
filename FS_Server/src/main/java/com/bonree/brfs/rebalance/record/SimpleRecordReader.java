package com.bonree.brfs.rebalance.record;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.bonree.brfs.rebalance.RecordReader;

public class SimpleRecordReader implements RecordReader<List<String>>, Closeable {


    private String fileName;

    private BufferedReader reader;

    private void mkDir(File file) {
        if (file.getParentFile().exists()) {
            file.mkdir();
        } else {
            mkDir(file.getParentFile());
            file.mkdir();
        }
    }

    public SimpleRecordReader(String fileName) throws FileNotFoundException {
        this.fileName = fileName;
        File file = new File(fileName);
        mkDir(file.getParentFile());
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    }

    @Override
    public List<String> readerRecord() throws IOException {
        List<String> results = new ArrayList<String>();
        String str = null;
        while ((str = reader.readLine()) != null) {
            results.add(str);
        }
        return results;
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

}
