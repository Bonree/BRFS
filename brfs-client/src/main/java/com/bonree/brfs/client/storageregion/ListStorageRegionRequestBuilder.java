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
package com.bonree.brfs.client.storageregion;

public class ListStorageRegionRequestBuilder {
    private boolean disableAllowed = true;
    private String prefix;
    private int maxKeys = Integer.MAX_VALUE;
    
    ListStorageRegionRequestBuilder() {}
    
    public void setDisableAllowed(boolean allowed) {
        this.disableAllowed = allowed;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public void setMaxKeys(int maxKeys) {
        this.maxKeys = maxKeys;
    }
    
    public ListStorageRegionRequest build() {
        return new ListStorageRegionRequest() {
            
            @Override
            public boolean disableAllowed() {
                return disableAllowed;
            }
            
            @Override
            public String getPrefix() {
                return prefix;
            }
            
            @Override
            public int getMaxKeys() {
                return maxKeys;
            }

        };
    }
}
