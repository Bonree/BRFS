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
package com.bonree.brfs.common.zookeeper.curator;

import javax.validation.constraints.Min;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CuratorConfig {
    
    @JsonProperty("addresses")
    private String addresses = "localhost:2181";
    
    @JsonProperty("acl")
    private boolean enableAcl = false;
    
    @JsonProperty("compress")
    private boolean enableCompression = true;
    
    @JsonProperty("user")
    private String zkUser;
    
    @JsonProperty("passwd")
    private String zkPasswd;
    
    @JsonProperty("sessionTimeoutMs")
    @Min(0)
    private int zkSessionTimeoutMs = 30000;
    
    @JsonProperty("authScheme")
    private String authScheme = "digest";

    public String getAddresses() {
        return addresses;
    }

    public void setAddresses(String addresses) {
        this.addresses = addresses;
    }

    public String getZkUser() {
        return zkUser;
    }

    public void setZkUser(String zkUser) {
        this.zkUser = zkUser;
    }

    public String getZkPasswd() {
        return zkPasswd;
    }

    public void setZkPasswd(String passwd) {
        this.zkPasswd = passwd;
    }

    public int getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public void setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
    }

    public String getAuthScheme() {
        return authScheme;
    }

    public void setAuthScheme(String authScheme) {
        this.authScheme = authScheme;
    }

    public boolean isEnableAcl() {
        return enableAcl;
    }

    public void setEnableAcl(boolean enableAcl) {
        this.enableAcl = enableAcl;
    }

    public boolean isEnableCompression() {
        return enableCompression;
    }

    public void setEnableCompression(boolean enableCompression) {
        this.enableCompression = enableCompression;
    }
    
}
