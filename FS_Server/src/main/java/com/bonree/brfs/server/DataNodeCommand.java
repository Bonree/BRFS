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

import com.bonree.brfs.common.plugin.NodeType;
import com.bonree.brfs.disknode.DataNodeIDModule;
import com.bonree.brfs.disknode.DataNodeModule;
import com.bonree.brfs.disknode.TaskModule;
import com.bonree.brfs.email.EmailModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.airline.Command;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "data",
    description = "Runs a data node"
)
public class DataNodeCommand extends BaseCommand {
    private static final Logger log = LoggerFactory.getLogger(DataNodeCommand.class);

    public DataNodeCommand() {
        super(log);
    }

    @Override
    protected List<Module> getModules() {
        return ImmutableList.of(
            new EmailModule().withNodeType(getNodeType()),
            new DataNodeModule(),
            new DataNodeIDModule(),
            new TaskModule());
    }

    @Override
    protected NodeType getNodeType() {
        return NodeType.DATA_NODE;
    }

}
