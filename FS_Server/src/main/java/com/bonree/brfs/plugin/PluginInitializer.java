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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.StringUtils;
import com.google.common.collect.ImmutableSet;

public class PluginInitializer {
    private static final Logger log = LoggerFactory.getLogger(PluginInitializer.class);

    public static <T> Set<T> loadPlugins(Class<T> serviceCls, PluginConfig config) {
        Path pluginDir = config.getPluginDir();
        if(!Files.exists(pluginDir)) {
            // TODO should throw a exception
            return ImmutableSet.of();
        }
        
        if(!Files.isDirectory(pluginDir)) {
            throw new IllegalStateException(StringUtils.format("configured plugin root dir[%s] should be directory", pluginDir));
        }
        
        try {
            Set<Path> loadList = Files.list(pluginDir)
                    .filter(Files::isDirectory)
                    .filter(p -> toBeLoaded(p, config))
                    .collect(Collectors.toSet());
            
            return loadPluginsFromFiles(loadList, serviceCls);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static boolean toBeLoaded(Path path, PluginConfig config) {
        Set<String> loadList = config.getLoadList();
        return loadList == null || loadList.isEmpty() || loadList.contains(path.getFileName().toString());
    }
    
    private static <T> Set<T> loadPluginsFromFiles(Set<Path> pluginDirs, Class<T> serviceCls) {
        ImmutableSet.Builder<T> builder = ImmutableSet.builder();
        for(Path plugin : pluginDirs) {
            if(!Files.isDirectory(plugin)) {
                throw new IllegalStateException(StringUtils.format("Plugin Dir[%s] should be directory", plugin));
            }
            
            log.info("loading plugin[{}]", plugin.getFileName());
            ServiceLoader.load(serviceCls, getPluginClassLoader(plugin)).forEach(s -> {
                builder.add(s);
            });
        }
        
        return builder.build();
    }
    
    private static ClassLoader getPluginClassLoader(Path plugin) {
        File[] jars = plugin.toFile().listFiles(f -> f.getName().endsWith(".jar"));
        URL[] urls = new URL[jars.length];
        
        try {
            int i = 0;
            for(File jar : jars) {
                URL url = jar.toURI().toURL();
                log.info("added URL[%s] for Plugin[%s]", url, plugin.getFileName());
                urls[i++] = url;
            }
            
            return new PluginClassLoader(urls, PluginInitializer.class.getClassLoader());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
