package com.bonree.brfs.gui.server;

import static com.bonree.brfs.common.http.rest.JaxrsBinder.jaxrs;

import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.gui.server.stats.StatReportor;
import com.bonree.brfs.gui.server.stats.StatResource;
import com.bonree.brfs.gui.server.stats.StatisticCollector;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;

public class StatModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(Discovery.class).to(GuiInnerClient.class).in(Singleton.class);
        binder.bind(StatReportor.class);
        binder.bind(StatisticCollector.class);
        jaxrs(binder).resource(StatResource.class);
    }

}
