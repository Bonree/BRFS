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

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClusterConfig {
    @JsonProperty("name")
    private String name = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_CLUSTER_NAME);
    
    @JsonProperty("datanode.group")
    private String dataNodeGroup = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME);
    
    @JsonProperty("regionnode.group")
    private String regionNodeGroup = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_REGION_SERVICE_GROUP_NAME);



    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataNodeGroup() {
        return dataNodeGroup;
    }

    public void setDataNodeGroup(String dataNodeGroup) {
        this.dataNodeGroup = dataNodeGroup;
    }

    public String getRegionNodeGroup() {
        return regionNodeGroup;
    }

    public void setRegionNodeGroup(String regionNodeGroup) {
        this.regionNodeGroup = regionNodeGroup;
    }
}
