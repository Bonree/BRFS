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

import static com.bonree.brfs.plugin.PluginDiscovery.discoverPlugins;
import static com.bonree.brfs.plugin.PluginDiscovery.writePluginServices;
import static com.google.common.collect.ImmutableList.toImmutableList;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.artifact.Artifact;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Streams;

import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;

public class PluginInitializer {
    private static final Logger log = LoggerFactory.getLogger(PluginInitializer.class);

    public static <T> Set<T> loadPlugins(Class<T> serviceCls, PluginConfig config) {
        return Streams
                .concat(buildPluginClassLoaderFromDirectory(config).stream(),
                        buildPluginClassLoaderFromBundle(config).stream())
                .map(cl -> loadPluginsFromClassLoader(cl, serviceCls)).flatMap(Set::stream)
                .collect(ImmutableSet.toImmutableSet());
    }
    
    private static List<ClassLoader> buildPluginClassLoaderFromDirectory(PluginConfig config) {
        Path pluginRoot = config.getPluginDir();
        return config.getLoadList()
                .stream()
                .map(pluginRoot::resolve)
                .map(Path::toFile)
                .map(dir -> {
                    log.info("load plugin from [{}]", dir);
                    
                    try {
                        return buildClassLoaderFromDirectory(dir);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(toImmutableList());
    }
    
    private static List<ClassLoader> buildPluginClassLoaderFromBundle(PluginConfig config) {
        ArtifactResolver resolver = new ArtifactResolver(config.getMavenLocalRepository(), config.getMavenRemoteRepository());
        return config.getPluginBundles()
                .stream()
                .map(plugin -> {
                    log.info("load plugin from [{}]", plugin);
                    
                    try {
                        return buildClassLoader(plugin, resolver);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(toImmutableList());
    }
    
    private static <T> Set<T> loadPluginsFromClassLoader(ClassLoader classLoader, Class<T> serviceCls) {
        ImmutableSet.Builder<T> builder = ImmutableSet.builder();
        ServiceLoader.load(serviceCls, classLoader).forEach(builder::add);
        
        return builder.build();
    }
    
    private static URLClassLoader buildClassLoader(String plugin, ArtifactResolver resolver)
            throws Exception
    {
        File file = new File(plugin);
        log.info("load plugin [{}]", file.getAbsolutePath());
        if (file.isFile() && (file.getName().equals("pom.xml") || file.getName().endsWith(".pom"))) {
            return buildClassLoaderFromPom(file, resolver);
        }
        if (file.isDirectory()) {
            return buildClassLoaderFromDirectory(file);
        }
        return buildClassLoaderFromCoordinates(plugin, resolver);
    }
    
    private static URLClassLoader buildClassLoaderFromDirectory(File dir) throws Exception {
        log.debug("Classpath for %s:", dir.getName());
        List<URL> urls = new ArrayList<>();
        for (File file : listFiles(dir)) {
            log.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }
    
    private static URLClassLoader buildClassLoaderFromCoordinates(String coordinates, ArtifactResolver resolver) throws Exception {
        Artifact rootArtifact = new DefaultArtifact(coordinates);
        List<Artifact> artifacts = resolver.resolveArtifacts(rootArtifact);
        return createClassLoader(artifacts, rootArtifact.toString());
    }
    
    private static URLClassLoader buildClassLoaderFromPom(File pomFile, ArtifactResolver resolver)
            throws Exception
    {
        List<Artifact> artifacts = resolver.resolvePom(pomFile);
        URLClassLoader classLoader = createClassLoader(artifacts, pomFile.getPath());

        Artifact artifact = artifacts.get(0);
        Set<String> plugins = discoverPlugins(artifact, classLoader);
        if (!plugins.isEmpty()) {
            writePluginServices(plugins, artifact.getFile());
        }

        return classLoader;
    }
    
    private static URLClassLoader createClassLoader(List<Artifact> artifacts, String name)
            throws IOException
    {
        log.debug("Classpath for %s:", name);
        List<URL> urls = new ArrayList<>();
        for (Artifact artifact : sortedArtifacts(artifacts)) {
            if (artifact.getFile() == null) {
                throw new RuntimeException("Could not resolve artifact: " + artifact);
            }
            File file = artifact.getFile().getCanonicalFile();
            log.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }
    
    private static URLClassLoader createClassLoader(List<URL> urls)
    {
        return new PluginClassLoader(urls, PluginInitializer.class.getClassLoader());
    }
    
    private static List<Artifact> sortedArtifacts(List<Artifact> artifacts)
    {
        List<Artifact> list = new ArrayList<>(artifacts);
        Collections.sort(list, Ordering.natural().nullsLast().onResultOf(Artifact::getFile));
        return list;
    }
    
    private static List<File> listFiles(File installedPluginsDir) {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                Arrays.sort(files);
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
