package com.bonree.brfs.gui.server;

import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.gui.server.resource.GuiResourceConfig;
import com.bonree.brfs.gui.server.resource.GuiResourceMaintainer;
import com.bonree.brfs.gui.server.resource.ResourceHandlerInterface;
import com.bonree.brfs.gui.server.resource.impl.GuiFileMaintainer;
import com.bonree.brfs.gui.server.resource.impl.ResourceHandler;
import com.bonree.brfs.gui.server.resource.maintain.ResourceRequestMaintainer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

public class ResourceModule implements Module {
    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "resource", GuiResourceConfig.class);
        binder.bind(ResourceHandlerInterface.class).to(ResourceHandler.class).in(Scopes.SINGLETON);
        binder.bind(GuiResourceMaintainer.class).to(GuiFileMaintainer.class).in(Scopes.SINGLETON);
        binder.bind(LifeCycle.class).to(ResourceRequestMaintainer.class).in(Scopes.SINGLETON);
    }
}
