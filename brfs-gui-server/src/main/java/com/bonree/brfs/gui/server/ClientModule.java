package com.bonree.brfs.gui.server;

import com.bonree.brfs.client.BRFSClientBuilder;
import com.bonree.brfs.client.utils.SocketChannelSocketFactory;
import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import java.time.Duration;
import java.util.Properties;
import okhttp3.OkHttpClient;

public class ClientModule implements Module {
    private Properties prop;

    public ClientModule(Properties prop) {
        this.prop = prop;
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(Properties.class).toInstance(prop);
        // 资源配置信息
        JsonConfigProvider.bind(binder, "resource", GuiResourceConfig.class);
        // brfs集群配置信息
        JsonConfigProvider.bind(binder, "brfs", BrfsConfig.class);
        // http请求配置信息
        JsonConfigProvider.bind(binder, "http", HttpConfig.class);
    }

    @Provides
    public OkHttpClient getHttpClient(BrfsConfig brfsConfig, HttpConfig httpConfig) {
        OkHttpClient httpClient = new OkHttpClient.Builder()
            .addNetworkInterceptor(
                new BRFSClientBuilder.AuthorizationIterceptor(brfsConfig.getUsername(), brfsConfig.getPassword()))
            .socketFactory(new SocketChannelSocketFactory())
            .callTimeout(Duration.ofSeconds(httpConfig.getRequestTimeout()))
            .connectTimeout(Duration.ofSeconds(httpConfig.getConnectTimeout()))
            .readTimeout(Duration.ofSeconds(httpConfig.getReadTimeout()))
            .writeTimeout(Duration.ofSeconds(httpConfig.getWriteTimeout()))
            .build();

        return httpClient;
    }

}
