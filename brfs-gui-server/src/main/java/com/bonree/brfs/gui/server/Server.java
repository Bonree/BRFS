package com.bonree.brfs.gui.server;

import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

import javax.inject.Singleton;
import javax.servlet.Filter;

import com.bonree.brfs.gui.server.zookeeper.ZookeeperResource;
import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.event.client.EventClient;
import com.facebook.airlift.event.client.NullEventClient;
import com.facebook.airlift.http.server.HttpServer;
import com.facebook.airlift.http.server.HttpServerModule;
import com.facebook.airlift.http.server.TheServlet;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.node.NodeInfo;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;

/**
 * Hello world!
 *
 */
public class Server {
    
    public static void main(String[] args) {
        Injector injector = new Bootstrap(
                new HttpServerModule(),
                new JaxrsModule(),
                binder -> {
                    binder.bind(EventClient.class).to(NullEventClient.class).in(Singleton.class);
                    binder.bind(NodeInfo.class).toInstance(new NodeInfo("env"));

//                    jaxrsBinder(binder).bind(ZookeeperResource.class);
                    jaxrsBinder(binder).bind(DashBoardResource.class);
                    jaxrsBinder(binder).bind(SystemMetricsResource.class);
                    
                    newSetBinder(binder, Filter.class, TheServlet.class).addBinding().to(HeaderFilter.class);
                })
                .doNotInitializeLogging()
                .requireExplicitBindings(false)
                .setRequiredConfigurationProperties(ImmutableMap.of("http-server.http.port", "9500"))
                .initialize();

        injector.getInstance(HttpServer.class);
    }
}
