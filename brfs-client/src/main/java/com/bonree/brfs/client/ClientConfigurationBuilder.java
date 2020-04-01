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
package com.bonree.brfs.client;

import java.net.URI;
import java.time.Duration;

public class ClientConfigurationBuilder {
    private URI[] addresses;
    private String user;
    private String passwd;
    private Duration discoveryExpire;
    private Duration discoveryRefresh;
    private Duration storageRegionCacheExpire;
    private Duration storageRegionCacheRefresh;
    private int dataPackageSize = 16 * 1024;
    private int connectionPoolSize;
    private int threadNum;

    ClientConfigurationBuilder() {
    }
    
    public void setRegionNodeAddresses(URI[] addresses) {
        this.addresses = addresses;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }
    
    public void setDiscoveryExpiredDuration(Duration expire) {
        this.discoveryExpire = expire;
    }
    
    public void setDiscoreryRefreshDuration(Duration refresh) {
        this.discoveryRefresh = refresh;
    }
    
    public void setStorageRegionCacheExpiredDuration(Duration expire) {
        this.storageRegionCacheExpire = expire;
    }
    
    public void setStorageRegionCacheRefreshDuration(Duration refresh) {
        this.storageRegionCacheRefresh = refresh;
    }
    
    public void setDataPackageSize(int size) {
        this.dataPackageSize = size;
    }

    public void setConnectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public ClientConfiguration build() {
        return new ClientConfiguration() {
            
            @Override
            public String getUser() {
                return user;
            }
            
            @Override
            public int getThreadNum() {
                return threadNum;
            }
            
            @Override
            public String getPasswd() {
                return passwd;
            }
            
            @Override
            public int getConnectionPoolSize() {
                return connectionPoolSize;
            }

            @Override
            public URI[] getRegionNodeAddresses() {
                return addresses;
            }

            @Override
            public Duration getDiscoveryExpiredDuration() {
                return discoveryExpire;
            }

            @Override
            public Duration getDiscoreryRefreshDuration() {
                return discoveryRefresh;
            }

            @Override
            public Duration getStorageRegionCacheExpireDuration() {
                return storageRegionCacheExpire;
            }

            @Override
            public Duration getStorageRegionCacheRefreshDuration() {
                return storageRegionCacheRefresh;
            }
            
            @Override
            public int getDataPackageSize() {
                return dataPackageSize;
            }
        };
    }
}
