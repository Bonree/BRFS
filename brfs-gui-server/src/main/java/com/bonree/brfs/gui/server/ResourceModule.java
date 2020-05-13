package com.bonree.brfs.gui.server;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.gui.server.resource.GuiResourceMaintainer;
import com.bonree.brfs.gui.server.resource.ResourceHandlerInterface;
import com.bonree.brfs.gui.server.resource.impl.GuiFileMaintainer;
import com.bonree.brfs.gui.server.resource.impl.ResourceHandler;
import com.bonree.brfs.gui.server.resource.maintain.ResourceRequestMaintainer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

public class ResourceModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(ResourceHandlerInterface.class).to(ResourceHandler.class).in(Scopes.SINGLETON);
        binder.bind(GuiResourceMaintainer.class).to(GuiFileMaintainer.class).in(Scopes.SINGLETON);
        binder.bind(ResourceRequestMaintainer.class).in(Scopes.SINGLETON);
    }

}
