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
package com.bonree.brfs.metrics.usage;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CpuUsageInfo {
    private final int total;
    private final int system;
    private final int user;
    private final int steal;
    private final int iowait;
    
    @JsonCreator
    public CpuUsageInfo(
            @JsonProperty("total") int total,
            @JsonProperty("system") int system,
            @JsonProperty("user") int user,
            @JsonProperty("steal") int steal,
            @JsonProperty("iowait") int iowait) {
        this.total = total;
        this.system = system;
        this.user = user;
        this.steal = steal;
        this.iowait = iowait;
    }

    @JsonProperty("total")
    public int getTotal() {
        return total;
    }

    @JsonProperty("system")
    public int getSystem() {
        return system;
    }

    @JsonProperty("user")
    public int getUser() {
        return user;
    }

    @JsonProperty("steal")
    public int getSteal() {
        return steal;
    }

    @JsonProperty("iowait")
    public int getIowait() {
        return iowait;
    }
    
    @Override
    public String toString() {
        return toStringHelper(getClass())
                .add("total", total)
                .add("system", system)
                .add("user", user)
                .add("steal", steal)
                .add("iowait", iowait)
                .toString();
    }
}
