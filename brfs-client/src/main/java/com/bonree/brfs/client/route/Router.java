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

package com.bonree.brfs.client.route;

import static java.util.function.Function.identity;

import com.bonree.brfs.client.ClientException;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Router {
    private final RouterClient routerClient;

    private final LoadingCache<String, Map<String, SecondServerID>> secondServerIds;
    private final LoadingCache<String, Map<String, NormalRouterNode>> normalUpdates;
    private final LoadingCache<String, Map<String, VirtualRouterNode>> virtualUpdates;

    public Router(RouterClient routerClient) {
        this.routerClient = routerClient;
        this.secondServerIds = CacheBuilder.newBuilder()
                                           .expireAfterWrite(10, TimeUnit.SECONDS)
                                           .refreshAfterWrite(5, TimeUnit.SECONDS)
                                           .build(new SecondServerIdLoader());

        this.normalUpdates = CacheBuilder.newBuilder()
                                         .expireAfterWrite(10, TimeUnit.SECONDS)
                                         .refreshAfterWrite(5, TimeUnit.SECONDS)
                                         .build(new NormalUpdateLoader());

        this.virtualUpdates = CacheBuilder.newBuilder()
                                          .expireAfterWrite(10, TimeUnit.SECONDS)
                                          .refreshAfterWrite(5, TimeUnit.SECONDS)
                                          .build(new VirtualUpdateLoader());
    }

    public Iterable<URI> getServerLocation(String srName, String uuid, List<String> secondServerIdList,
                                           Map<URI, Integer> uriIndex) {
        try {
            Map<String, SecondServerID> secondServers = secondServerIds.get(srName);
            Map<String, NormalRouterNode> normalMapper = normalUpdates.get(srName);
            Map<String, VirtualRouterNode> virtualMapper = virtualUpdates.get(srName);

            int code = RouteAnalysis.indexCode(uuid);
            return Iterables.filter(
                Iterables.transform(
                    secondServerIdList,
                    serverId -> {
                        String finalId = finalServerId(code, serverId, secondServerIdList, normalMapper, virtualMapper);
                        if (finalId == null) {
                            return null;
                        }

                        SecondServerID secondId = secondServers.get(finalId);
                        if (secondId == null) {
                            return null;
                        }

                        try {
                            URI uri = new URI("http", null, secondId.getHost(), secondId.getReadPort(), null, null, null);
                            uriIndex.put(uri, secondServerIdList.indexOf(serverId) + 1);

                            return uri;
                        } catch (URISyntaxException e) {
                            throw new RuntimeException(e);
                        }
                    }),
                Objects::nonNull);
        } catch (ExecutionException cause) {
            throw new ClientException(cause, "get update of router error");
        }
    }

    private String finalServerId(
        int code,
        String serverId,
        List<String> secondServerIdList,
        Map<String, NormalRouterNode> normalMapper,
        Map<String, VirtualRouterNode> virtualMapper) {
        String secondId = null;
        if (serverId.startsWith("3")) {
            //virtual id
            VirtualRouterNode update = virtualMapper.get(serverId);
            if (update == null) {
                return serverId;
            }

            secondId = update.getNewSecondId();
        }

        if (Strings.isNullOrEmpty(secondId)) {
            return serverId;
        }

        NormalRouterNode normalUpdate;
        while ((normalUpdate = normalMapper.get(serverId)) != null) {
            secondId = RouteAnalysis.analysisNormal(code, secondId, secondServerIdList, normalUpdate);
        }

        return secondId;
    }

    private class SecondServerIdLoader extends CacheLoader<String, Map<String, SecondServerID>> {

        @Override
        public Map<String, SecondServerID> load(String srName) throws Exception {
            List<SecondServerID> secondServerIDs = routerClient.getSecondServerId(srName);
            return secondServerIDs.stream().collect(Collectors.toMap(SecondServerID::getSecondServerId, identity()));
        }

    }

    private class NormalUpdateLoader extends CacheLoader<String, Map<String, NormalRouterNode>> {

        @Override
        public Map<String, NormalRouterNode> load(String srName) throws Exception {
            List<NormalRouterNode> normalUpdates = routerClient.getNormalRouter(srName);
            return normalUpdates.stream().collect(Collectors.toMap(NormalRouterNode::getBaseSecondId, identity()));
        }

    }

    private class VirtualUpdateLoader extends CacheLoader<String, Map<String, VirtualRouterNode>> {

        @Override
        public Map<String, VirtualRouterNode> load(String srName) throws Exception {
            List<VirtualRouterNode> virtualUpdates = routerClient.getVirtualRouter(srName);
            return virtualUpdates.stream().collect(Collectors.toMap(VirtualRouterNode::getVirtualId, identity()));
        }

    }
}
