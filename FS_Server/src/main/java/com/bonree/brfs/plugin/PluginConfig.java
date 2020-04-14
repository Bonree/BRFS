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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.bonree.brfs.common.utils.StringUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import io.airlift.resolver.ArtifactResolver;

public class PluginConfig {
    
    @JsonProperty("dir")
    private String pluginDir = "plugins";
    
    @JsonProperty("loadList")
    private List<String> loadList = ImmutableList.of();
    
    @JsonProperty("bundles")
    private List<String> pluginBundles = ImmutableList.of();
    
    @JsonProperty("maven.repo.local")
    private String mavenLocalRepository = ArtifactResolver.USER_LOCAL_REPO;
    
    @JsonProperty("maven.repo.remote")
    private List<String> mavenRemoteRepository = ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI);

    public Path getPluginDir() {
        return Paths.get(pluginDir);
    }

    public void setPluginDir(String pluginDir) {
        if(!StringUtils.isNullorEmpty(pluginDir)) {
            this.pluginDir = pluginDir;
        }
    }

    public List<String> getLoadList() {
        return loadList;
    }

    public void setLoadList(List<String> loadList) {
        this.loadList = loadList;
    }
    
    public void setPluginBundles(List<String> pluginBundles) {
        this.pluginBundles = pluginBundles;
    }
    
    public List<String> getPluginBundles() {
        return pluginBundles;
    }

    public String getMavenLocalRepository() {
        return mavenLocalRepository;
    }

    public void setMavenLocalRepository(String mavenLocalRepository) {
        this.mavenLocalRepository = mavenLocalRepository;
    }

    public List<String> getMavenRemoteRepository() {
        return mavenRemoteRepository;
    }

    public void setMavenRemoteRepository(List<String> mavenRemoteRepository) {
        this.mavenRemoteRepository = mavenRemoteRepository;
    }
}
