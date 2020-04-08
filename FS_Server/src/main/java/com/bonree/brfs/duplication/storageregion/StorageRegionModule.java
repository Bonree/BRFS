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
package com.bonree.brfs.duplication.storageregion;

import static com.bonree.brfs.common.http.rest.JaxrsBinder.jaxrs;

import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.duplication.storageregion.exception.StorageRegionExistedExceptionMapper;
import com.bonree.brfs.duplication.storageregion.exception.StorageRegionNonexistentExceptionMapper;
import com.bonree.brfs.duplication.storageregion.exception.StorageRegionStateExceptionMapper;
import com.bonree.brfs.duplication.storageregion.impl.DefaultStorageRegionManager;
import com.bonree.brfs.duplication.storageregion.impl.ZkStorageRegionIdBuilder;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

public class StorageRegionModule implements Module {

    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "storage", StorageRegionConfig.class);
        
        binder.bind(StorageRegionManager.class).to(DefaultStorageRegionManager.class);
        binder.bind(StorageRegionIdBuilder.class).to(ZkStorageRegionIdBuilder.class).in(Scopes.SINGLETON);
        
        binder.requestStaticInjection(StorageRegionProperties.class);
        
        jaxrs(binder).resource(StorageRegionResource.class);
        jaxrs(binder).resource(StorageRegionExistedExceptionMapper.class);
        jaxrs(binder).resource(StorageRegionNonexistentExceptionMapper.class);
        jaxrs(binder).resource(StorageRegionStateExceptionMapper.class);
    }

}
