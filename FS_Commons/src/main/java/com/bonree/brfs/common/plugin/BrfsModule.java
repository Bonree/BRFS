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
package com.bonree.brfs.common.plugin;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.atomic.AtomicReference;

import com.google.inject.Binder;
import com.google.inject.Module;

import io.airlift.airline.Cli;

public abstract class BrfsModule implements Module {
    private final AtomicReference<NodeType> nodeTypeRef = new AtomicReference<>();
    
    public BrfsModule withNodeType(NodeType nodeType) {
        if(!nodeTypeRef.compareAndSet(null, requireNonNull(nodeType))) {
            throw new IllegalStateException("node type should be set just once");
        }
        
        return this;
    }

    @Override
    public void configure(Binder binder) {
        NodeType nodeType = nodeTypeRef.get();
        if(nodeType == null) {
            throw new IllegalStateException("No node type is specified");
        }
        
        configure(nodeType, binder);
    }
    
    protected abstract void configure(NodeType nodeType, Binder binder);
    
    public void addCommands(Cli.CliBuilder<Runnable> builder) {}
}
