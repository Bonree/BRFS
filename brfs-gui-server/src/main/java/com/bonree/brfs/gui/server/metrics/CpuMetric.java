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

package com.bonree.brfs.gui.server.metrics;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CpuMetric {
    private final long time;
    private final double total;
    private final double system;
    private final double user;
    private final double steal;
    private final double iowait;

    @JsonCreator
    public CpuMetric(
        @JsonProperty("time") long time,
        @JsonProperty("total") double total,
        @JsonProperty("system") double system,
        @JsonProperty("user") double user,
        @JsonProperty("steal") double steal,
        @JsonProperty("iowait") double iowait) {
        this.time = time;
        this.total = total;
        this.system = system;
        this.user = user;
        this.steal = steal;
        this.iowait = iowait;
    }

    @JsonProperty("time")
    public long getTime() {
        return time;
    }

    @JsonProperty("total")
    public double getTotal() {
        return total;
    }

    @JsonProperty("system")
    public double getSystem() {
        return system;
    }

    @JsonProperty("user")
    public double getUser() {
        return user;
    }

    @JsonProperty("steal")
    public double getSteal() {
        return steal;
    }

    @JsonProperty("iowait")
    public double getIowait() {
        return iowait;
    }

    @Override
    public String toString() {
        return toStringHelper(getClass())
            .add("time", time)
            .add("total", total)
            .add("system", system)
            .add("user", user)
            .add("steal", steal)
            .add("iowait", iowait)
            .toString();
    }
}
