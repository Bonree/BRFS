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

public class NetworkUsageInfo {
    private final long sendBytesPerSecond;
    private final long receiveBytesPerSecond;
    
    @JsonCreator
    public NetworkUsageInfo(
            @JsonProperty("TXBytesPS") long sendBytesPerSecond,
            @JsonProperty("RXBytesPS") long receiveBytesPerSecond) {
        this.sendBytesPerSecond = sendBytesPerSecond;
        this.receiveBytesPerSecond = receiveBytesPerSecond;
    }

    @JsonProperty("TXBytesPS")
    public long getSendBytesPerSecond() {
        return sendBytesPerSecond;
    }

    @JsonProperty("RXBytesPS")
    public long getReceiveBytesPerSecond() {
        return receiveBytesPerSecond;
    }
    
    @Override
    public String toString() {
        return toStringHelper(getClass())
                .add("sendBytesPerSecond", sendBytesPerSecond)
                .add("receiveBytesPerSecond", receiveBytesPerSecond)
                .toString();
    }
}
