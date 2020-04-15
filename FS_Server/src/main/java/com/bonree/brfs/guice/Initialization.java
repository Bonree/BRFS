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
package com.bonree.brfs.guice;

import com.bonree.brfs.common.guice.ConfigModule;
import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.http.rest.JaxrsModule;
import com.bonree.brfs.common.jackson.JsonModule;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.plugin.BrfsModule;
import com.bonree.brfs.common.plugin.NodeType;
import com.bonree.brfs.common.zookeeper.curator.CuratorModule;
import com.bonree.brfs.plugin.PluginConfig;
import com.bonree.brfs.plugin.PluginInitializer;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;

public class Initialization {
    
    public static Injector makeSetupInjector() {
        return Guice.createInjector(ImmutableList.of(
                new ConfigModule(),
                new JsonModule(),
                new PropertiesModule(),
                binder -> {
                    JsonConfigProvider.bind(binder, "brfs.plugins", PluginConfig.class);
                }
                ));
    }

    public static Injector makeInjectorWithModules(NodeType nodeType, final Injector baseInjector, Iterable<? extends Module> modules) {
        ImmutableList.Builder<Module> defaultModules = ImmutableList.<Module>builder()
        .add(new LifecycleModule())
        .add(new CuratorModule())
        .add(new JaxrsModule())
        .add(baseInjector.getInstance(InitModule.class));
        
        Module nodeModule = Modules.override(defaultModules.build()).with(modules);
        
        PluginConfig pluginConfig = baseInjector.getInstance(PluginConfig.class);
        ImmutableList.Builder<Module> pluginModules = ImmutableList.builder();
        for(BrfsModule pluginModule : PluginInitializer.loadPlugins(BrfsModule.class, pluginConfig)){
            pluginModules.add(pluginModule.withNodeType(nodeType));
        }
        
        return Guice.createInjector(Modules.override(nodeModule).with(pluginModules.build()));
    }
}
