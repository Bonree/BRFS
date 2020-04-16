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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class Router {
    private final LoadingCache<String, Map<String, SecondServerID>> secondServerIds;
    
    public Router(RouterClient routerClient) {
        this.secondServerIds = CacheBuilder.newBuilder()
                .build(new SecondServerIdLoader(routerClient));
    }
    
    public List<URI> getServerLocation(String srName, String uuid, List<String> secondServerIdList) {
        
        
        String sid = null;
        
        List<NormalRouterNode> normals;
        List<VirtualRouterNode> virtuals;
        
        try {
            Map<String, SecondServerID> maps = secondServerIds.get(srName);
            SecondServerID id = maps.get(sid);
            id.getServiceId();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
    
    private String finalServerId(String uuid, String serverId) {
        //TODO
        return serverId;
    }
    
    private static class SecondServerIdLoader extends CacheLoader<String, Map<String, SecondServerID>> {
        private final RouterClient routerClient;
        
        public SecondServerIdLoader(RouterClient routerClient) {
            this.routerClient = routerClient;
        }

        @Override
        public Map<String, SecondServerID> load(String srName) throws Exception {
            List<SecondServerID> secondServerIDs = routerClient.getSecondServerId(srName);
            return secondServerIDs.stream()
                    .collect(Collectors.toMap(SecondServerID::getSecondServerId, identity()));
        }
        
    }
}
