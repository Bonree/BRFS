package com.bonree.brfs.client.discovery;

import java.io.Closeable;
import java.net.URI;

public interface NodeSelector extends Closeable {
    public Iterable<URI> getNodeHttpLocations(Discovery.ServiceType type);

    public Iterable<URI> getNodeHttpLocations(Discovery.ServiceType type, String srName);

}
