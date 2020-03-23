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
import java.util.Set;

import com.bonree.brfs.common.utils.StringUtils;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PluginConfig {
    
    @JsonProperty("dir")
    private String pluginDir = "plugins";
    
    @JsonProperty("loadList")
    private Set<String> loadList;

    public Path getPluginDir() {
        return Paths.get(pluginDir);
    }

    public void setPluginDir(String pluginDir) {
        if(!StringUtils.isNullorEmpty(pluginDir)) {
            this.pluginDir = pluginDir;
        }
    }

    public Set<String> getLoadList() {
        return loadList;
    }

    public void setLoadList(Set<String> loadList) {
        this.loadList = loadList;
    }
    
    
}
