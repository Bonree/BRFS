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

import java.net.URI;

import javax.inject.Singleton;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.uri.internal.JerseyUriBuilder;

import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.http.HttpServer;
import com.bonree.brfs.common.http.HttpServerConfig;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.plugin.BrfsModule;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import io.netty.handler.ssl.SslContext;

public class NettyHttpServerModule implements BrfsModule {

    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "httpserver", NettyHttpServerConfig.class);
        
        binder.bind(HttpServer.class).to(NettyHttpServer.class);
        binder.bind(HttpServerConfig.class).to(NettyHttpServerConfig.class).in(Scopes.SINGLETON);
        
        LifecycleModule.register(binder, HttpServer.class);
    }
    
    @Provides
    @RootUri
    public URI getRootUri(NettyHttpServerConfig httpConfig) {
        return new JerseyUriBuilder().path("/").build();
    }
    
    @Provides
    public SslContext getSslContext() {
        //TODO build SslContext according to http configurations
        return null;
    }

    @Provides
    @Singleton
    public NettyHttpContainer getContainer(ResourceConfig configuration) {
        return NettyHttpContainer.create(configuration);
    }
    
    @Provides
    @Singleton
    public JerseyServerHandler getServerHandler(
            @RootUri URI uri,
            NettyHttpContainer container,
            ResourceConfig resourceConfig) {
        return new JerseyServerHandler(uri, container, resourceConfig);
    }
}
