/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bonree.brfs.client.discovery;

import com.bonree.brfs.client.discovery.Discovery.ServiceType;
import com.bonree.brfs.client.ranker.Ranker;
import com.google.common.collect.Iterables;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Collectors;

public class NodeSelector implements Closeable {
    private final Discovery discovery;
    private final Ranker<ServerNode> nodeRanker;

    public NodeSelector(Discovery discovery, Ranker<ServerNode> nodeRanker) {
        this.discovery = discovery;
        this.nodeRanker = nodeRanker;
    }

    public Iterable<URI> getNodeHttpLocations(ServiceType type) {
        return getNodeLocations(type, null, "http");
    }

    public Iterable<URI> getNodeHttpLocations(ServiceType type, String srName) {
        return getNodeLocations(type, srName, "http");
    }

    public Iterable<URI> getNodeLocations(ServiceType type, String srName, String scheme) {
        return Iterables.transform(
            nodeRanker.rank(discovery.getServiceList(type)
                                .stream()
                                .filter(node -> {
                                    if (srName == null) {
                                        return true;
                                    }

                                    if (node.getAllowed() != null && !node.getAllowed().isEmpty()) {
                                        return node.getAllowed().contains(srName);
                                    }

                                    if (node.getForbidden() != null && !node.getForbidden().isEmpty()) {
                                        return !node.getForbidden().contains(srName);
                                    }

                                    return true;
                                })
                                .collect(Collectors.toList())),
            node -> buildUri(scheme, node.getHost(), node.getPort()));
    }

    private URI buildUri(String scheme, String host, int port) {
        try {
            return new URI(scheme, null, host, port, null, null, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() throws IOException {
        discovery.close();
    }

}
