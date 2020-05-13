package com.bonree.brfs.gui.server;

import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

import com.bonree.brfs.common.guice.ConfigModule;
import com.bonree.brfs.common.jackson.JsonModule;
import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.gui.server.resource.GuiResourceMaintainer;
import com.bonree.brfs.gui.server.resource.maintain.ResourceRequestMaintainer;
import com.bonree.brfs.gui.server.zookeeper.ZookeeperResource;
import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.event.client.EventClient;
import com.facebook.airlift.event.client.NullEventClient;
import com.facebook.airlift.http.server.HttpServer;
import com.facebook.airlift.http.server.HttpServerModule;
import com.facebook.airlift.http.server.TheServlet;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.node.NodeInfo;
import com.google.inject.Injector;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.inject.Singleton;
import javax.servlet.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 */
public class Server {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    private static final String SYS_PROPERTIES_FILE = "configuration.file";

    @SuppressWarnings("checkstyle:WhitespaceAround")
    public static void main(String[] args) throws Exception {
        Properties prop = get();
        Injector injector = new Bootstrap(
            new ConfigModule(),
            new JsonModule(),
            new ClientModule(prop),
            new HttpServerModule(),
            new JaxrsModule(),
            new ResourceModule(),
            binder -> {

                binder.bind(EventClient.class).to(NullEventClient.class).in(Singleton.class);
                binder.bind(NodeInfo.class).toInstance(new NodeInfo("env"));

                jaxrsBinder(binder).bind(ZookeeperResource.class);
                jaxrsBinder(binder).bind(DashBoardResource.class);
                //jaxrsBinder(binder).bind(SystemMonitorResource.class);
                //                jaxrsBinder(binder).bind(CatalogGuiResource.class);
                newSetBinder(binder, Filter.class, TheServlet.class).addBinding().to(HeaderFilter.class);
            }

        )
            .doNotInitializeLogging()
            .requireExplicitBindings(false)
            .setRequiredConfigurationProperties(toMap(prop))
            .initialize();

        injector.getInstance(HttpServer.class);
        ResourceRequestMaintainer resourceLifeCycle = injector.getInstance(ResourceRequestMaintainer.class);
        GuiResourceMaintainer maintainer = injector.getInstance(GuiResourceMaintainer.class);
        resourceLifeCycle.start();
        maintainer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    maintainer.stop();
                    resourceLifeCycle.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }));
        LOG.info("server start successful !!");
    }

    private static Properties get() {
        String configFilePath = System.getProperty(SYS_PROPERTIES_FILE);
        if (configFilePath == null) {
            throw new RuntimeException(StringUtils.format(
                "No configuration file is specified by property[%s]",
                SYS_PROPERTIES_FILE));
        }

        File configFile = new File(configFilePath);
        if (!configFile.exists()) {
            throw new RuntimeException(StringUtils.format(
                "config file[%s] is not existed",
                configFile));
        }

        if (!configFile.isFile()) {
            throw new RuntimeException(StringUtils.format(
                "config file[%s] is not a regular file",
                configFile));
        }
        Properties prop = new Properties();
        try (InputStream stream = new BufferedInputStream(new FileInputStream(configFile))) {
            prop.load(new InputStreamReader(stream, StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            // It's impossible to come here!
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return prop;
    }

    private static Map<String, String> toMap(Properties prop) {
        if (prop.isEmpty()) {
            return new HashMap<>(0);
        }
        Map<String, String> map = new HashMap<>();
        prop.forEach((x, y) -> {
            map.put((String) x, (String) y);
        });
        return map;
    }

}
