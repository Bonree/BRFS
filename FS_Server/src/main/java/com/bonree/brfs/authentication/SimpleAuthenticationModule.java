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
package com.bonree.brfs.authentication;

import javax.inject.Singleton;

import org.apache.curator.framework.CuratorFramework;

import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;

public class SimpleAuthenticationModule implements Module {

    @Override
    public void configure(Binder binder) {}

    @Provides
    @Singleton
    public SimpleAuthentication getSimpleAuthentication(CuratorFramework zkClient, ZookeeperPaths paths, Lifecycle lifecycle) {
        SimpleAuthentication simpleAuthentication = SimpleAuthentication.getAuthInstance(
                paths.getBaseLocksPath(),
                zkClient);
        
        lifecycle.addLifeCycleObject(new Lifecycle.LifeCycleObject() {
            
            @Override
            public void start() throws Exception {
                simpleAuthentication.init(paths.getBaseUserPath());
                UserModel model = simpleAuthentication.getUser("root");
                if (model == null) {
                    throw new RuntimeException("server is not initialized");
                }
            }
            
            @Override
            public void stop() {
            }
            
        }, Lifecycle.Stage.INIT);
        
        return simpleAuthentication;
    }
}
