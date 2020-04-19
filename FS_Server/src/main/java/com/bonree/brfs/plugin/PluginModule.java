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
package com.bonree.brfs.plugin;

import java.util.Set;

import javax.inject.Singleton;

import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.plugin.BrfsModule;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;

public class PluginModule implements Module {

    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "brfs.plugins", PluginConfig.class);
    }

    @Provides
    @Singleton
    public Set<BrfsModule> getPlugins(PluginConfig config) {
        ImmutableSet.Builder<BrfsModule> pluginModules = ImmutableSet.builder();
        for(BrfsModule pluginModule : PluginInitializer.loadPlugins(BrfsModule.class, config)){
            pluginModules.add(pluginModule);
        }
        
        return pluginModules.build();
    }
}
