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

import javax.inject.Inject;

import org.slf4j.Logger;

import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.plugin.NodeType;
import com.bonree.brfs.guice.Initialization;
import com.google.inject.Injector;
import com.google.inject.Module;

public abstract class BaseCommand implements Runnable {
    private final Logger log;
    
    private Injector baseInjector;
    
    protected BaseCommand(Logger log) {
        this.log = log;
    }
    
    @Inject
    public void setBaseInjector(Injector baseInjector) {
        this.baseInjector = baseInjector;
    }
    
    protected abstract List<Module> getModules();
    
    protected abstract NodeType getNodeType();

    @Override
    public void run() {
        try {
            Injector injector = Initialization.makeInjectorWithModules(getNodeType(), baseInjector, getModules());
            Lifecycle lifeCycle = injector.getInstance(Lifecycle.class);
            
            try {
                lifeCycle.start();
            } catch (Throwable t) {
                log.error("FAILED to start up", t);
                System.exit(1);
            }
            
            try {
                lifeCycle.join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
