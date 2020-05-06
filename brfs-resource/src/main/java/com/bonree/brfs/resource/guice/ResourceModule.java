package com.bonree.brfs.resource.guice;

import com.bonree.brfs.common.plugin.BrfsModule;
import com.bonree.brfs.common.plugin.NodeType;
import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.resource.impl.SigarGather;
import com.google.inject.Binder;
import com.google.inject.Singleton;

public class ResourceModule extends BrfsModule {
    @Override
    protected void configure(NodeType nodeType, Binder binder) {
        binder.bind(ResourceCollectionInterface.class).to(SigarGather.class).in(Singleton.class);
    }
}
