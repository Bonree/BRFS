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
package com.bonree.brfs.server;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.authentication.SimpleAuthenticationModule;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RocksDBConfigs;
import com.bonree.brfs.duplication.RegionIDModule;
import com.bonree.brfs.duplication.RegionNodeModule;
import com.bonree.brfs.duplication.storageregion.StorageRegionModule;
import com.bonree.brfs.email.EmailModule;
import com.bonree.brfs.rocksdb.guice.RocksDBModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;

import io.airlift.airline.Command;

@Command(
        name = "region",
        description = "Runs a region node"
)
public class RegionNodeCommand extends BaseCommand {
    private static final Logger log = LoggerFactory.getLogger(RegionNodeCommand.class);

    private static final boolean ROCKSDB_SWITCH = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_SWITCH);

    public RegionNodeCommand() {
        super(log);
    }

    @Override
    protected List<Module> getModules() {
        return ImmutableList.of(
                new EmailModule(),
                new SimpleAuthenticationModule(),
                new StorageRegionModule(),
                new RegionNodeModule(),
                new RegionIDModule(),
                binder -> {
                    if(ROCKSDB_SWITCH) {
                        binder.install(new RocksDBModule());
                    }
                });
    }

}
