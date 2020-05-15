package com.bonree.brfs.gui.server.zookeeper;

import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;

public class ZookeeperModule implements Module {

    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "zookeeper", ZookeeperConfig.class);
        binder.bind(ZookeeperInfoTaker.class).to(ZookeeperInfoTakerImpl.class).in(Singleton.class);

        jaxrsBinder(binder).bind(ZookeeperResource.class);
    }
}
