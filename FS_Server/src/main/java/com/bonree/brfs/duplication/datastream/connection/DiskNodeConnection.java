package com.bonree.brfs.duplication.datastream.connection;

import com.bonree.brfs.disknode.client.DiskNodeClient;
import java.io.Closeable;

public interface DiskNodeConnection extends Closeable {
    String getRemoteAddress();

    int getRemotePort();

    boolean isValid();

    DiskNodeClient getClient();
}
