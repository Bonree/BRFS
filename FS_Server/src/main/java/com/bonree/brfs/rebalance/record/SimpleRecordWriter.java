package com.bonree.brfs.rebalance.record;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import com.bonree.brfs.rebalance.RecordWriter;

public class SimpleRecordWriter implements RecordWriter<String>, Closeable {

    private String fileName;

    private BufferedWriter writer;

    private final static String LINE_SEPARATOR = System.lineSeparator();

    private void mkDir(File file) {
        if (file.getParentFile().exists()) {
            file.mkdir();
        } else {
            mkDir(file.getParentFile());
            file.mkdir();
        }
    }

    public SimpleRecordWriter(String fileName) throws FileNotFoundException {
        this.fileName = fileName;
        File file = new File(fileName);
        mkDir(file.getParentFile());
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
    }

    @Override
    public void writeRecord(String input) throws IOException {
        writer.write(input);
        writer.write(LINE_SEPARATOR);
        writer.flush();
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

}
