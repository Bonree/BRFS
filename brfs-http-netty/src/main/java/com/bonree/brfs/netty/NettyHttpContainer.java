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
package com.bonree.brfs.netty;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import javax.ws.rs.core.Application;

import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.spi.ExecutorServiceProvider;
import org.glassfish.jersey.spi.ScheduledExecutorServiceProvider;

class NettyHttpContainer implements Container {

    private volatile ApplicationHandler appHandler;

    public static NettyHttpContainer create(Application application) {
        NettyHttpContainer container = new NettyHttpContainer(application);
        container.appHandler.onStartup(container);

        return container;
    }

    private NettyHttpContainer(Application application) {
        this.appHandler = new ApplicationHandler(application);
    }

    @Override
    public ResourceConfig getConfiguration() {
        return appHandler.getConfiguration();
    }

    @Override
    public ApplicationHandler getApplicationHandler() {
        return appHandler;
    }

    @Override
    public void reload() {
        reload(appHandler.getConfiguration());
    }

    @Override
    public void reload(ResourceConfig configuration) {
        appHandler.onShutdown(this);

        appHandler = new ApplicationHandler(configuration);
        appHandler.onReload(this);
        appHandler.onStartup(this);
    }

    /**
     * Get {@link java.util.concurrent.ExecutorService}.
     *
     * @return Executor service associated with this container.
     */
    ExecutorService getExecutorService() {
        return appHandler.getInjectionManager().getInstance(ExecutorServiceProvider.class).getExecutorService();
    }

    /**
     * Get {@link ScheduledExecutorService}.
     *
     * @return Scheduled executor service associated with this container.
     */
    ScheduledExecutorService getScheduledExecutorService() {
        return appHandler.getInjectionManager().getInstance(ScheduledExecutorServiceProvider.class)
                .getExecutorService();
    }
}
