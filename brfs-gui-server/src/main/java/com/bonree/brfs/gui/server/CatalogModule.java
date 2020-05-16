package com.bonree.brfs.gui.server;

import static com.bonree.brfs.common.http.rest.JaxrsBinder.jaxrs;

import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.gui.server.catalog.CatalogGuiResource;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;

public class CatalogModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(Discovery.class).to(GuiInnerClient.class).in(Singleton.class);
        jaxrs(binder).resource(CatalogGuiResource.class);
    }

}
